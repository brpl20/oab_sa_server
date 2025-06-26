import json
import boto3
import cv2
import numpy as np
from PIL import Image
import io
import os
from dotenv import load_dotenv
import face_recognition
from typing import List, Dict, Tuple
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LawyerFaceComparator:
    def __init__(self):
        """Initialize the face comparator with AWS S3 client"""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        self.bucket_name = 'oabapi-profile-pic'
        
    def download_image_from_s3(self, image_key: str) -> np.ndarray:
        """
        Download image from S3 and return as numpy array
        
        Args:
            image_key: The S3 key for the image
            
        Returns:
            numpy array of the image
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=image_key)
            image_data = response['Body'].read()
            
            # Convert to PIL Image
            pil_image = Image.open(io.BytesIO(image_data))
            
            # Convert to RGB if needed
            if pil_image.mode != 'RGB':
                pil_image = pil_image.convert('RGB')
            
            # Convert to numpy array
            image_array = np.array(pil_image)
            
            logger.info(f"Successfully downloaded image: {image_key}")
            return image_array
            
        except Exception as e:
            logger.error(f"Error downloading image {image_key}: {str(e)}")
            return None
    
    def extract_face_encoding(self, image: np.ndarray) -> np.ndarray:
        """
        Extract face encoding from image using face_recognition library
        
        Args:
            image: numpy array of the image
            
        Returns:
            Face encoding array or None if no face found
        """
        try:
            # Find face locations
            face_locations = face_recognition.face_locations(image)
            
            if not face_locations:
                logger.warning("No face found in image")
                return None
            
            # Get face encodings
            face_encodings = face_recognition.face_encodings(image, face_locations)
            
            if not face_encodings:
                logger.warning("Could not encode face")
                return None
            
            # Return the first face encoding (assuming one face per image)
            return face_encodings[0]
            
        except Exception as e:
            logger.error(f"Error extracting face encoding: {str(e)}")
            return None
    
    def compare_faces(self, encoding1: np.ndarray, encoding2: np.ndarray, 
                     tolerance: float = 0.6) -> Tuple[bool, float]:
        """
        Compare two face encodings
        
        Args:
            encoding1: First face encoding
            encoding2: Second face encoding
            tolerance: Face comparison tolerance (lower = more strict)
            
        Returns:
            Tuple of (is_match, distance)
        """
        try:
            # Calculate face distance
            distance = face_recognition.face_distance([encoding1], encoding2)[0]
            
            # Determine if it's a match
            is_match = distance <= tolerance
            
            return is_match, distance
            
        except Exception as e:
            logger.error(f"Error comparing faces: {str(e)}")
            return False, 1.0
    
    def process_lawyers_json(self, json_file_path: str, tolerance: float = 0.6) -> Dict:
        """
        Process a JSON file containing lawyers with the same name
        
        Args:
            json_file_path: Path to JSON file with lawyer data
            tolerance: Face comparison tolerance
            
        Returns:
            Dictionary with comparison results
        """
        try:
            # Load JSON data
            with open(json_file_path, 'r', encoding='utf-8') as f:
                lawyers_data = json.load(f)
            
            if not lawyers_data:
                return {"error": "No lawyer data found"}
            
            lawyer_name = lawyers_data[0].get('full_name', 'Unknown')
            logger.info(f"Processing {len(lawyers_data)} lawyers named: {lawyer_name}")
            
            # Download images and extract encodings
            lawyer_encodings = []
            for lawyer in lawyers_data:
                profile_pic = lawyer.get('profile_picture')
                if not profile_pic:
                    logger.warning(f"No profile picture for lawyer ID: {lawyer.get('id')}")
                    lawyer_encodings.append(None)
                    continue
                
                # Download image
                image = self.download_image_from_s3(profile_pic)
                if image is None:
                    lawyer_encodings.append(None)
                    continue
                
                # Extract face encoding
                encoding = self.extract_face_encoding(image)
                lawyer_encodings.append(encoding)
            
            # Compare all pairs
            results = {
                "lawyer_name": lawyer_name,
                "total_lawyers": len(lawyers_data),
                "successful_encodings": sum(1 for enc in lawyer_encodings if enc is not None),
                "comparisons": [],
                "groups": []
            }
            
            # Perform pairwise comparisons
            for i in range(len(lawyers_data)):
                for j in range(i + 1, len(lawyers_data)):
                    lawyer1 = lawyers_data[i]
                    lawyer2 = lawyers_data[j]
                    encoding1 = lawyer_encodings[i]
                    encoding2 = lawyer_encodings[j]
                    
                    if encoding1 is None or encoding2 is None:
                        comparison_result = {
                            "lawyer1_id": lawyer1.get('id'),
                            "lawyer1_oab": lawyer1.get('oab_id'),
                            "lawyer2_id": lawyer2.get('id'),
                            "lawyer2_oab": lawyer2.get('oab_id'),
                            "is_match": False,
                            "distance": None,
                            "error": "Could not extract face encoding"
                        }
                    else:
                        is_match, distance = self.compare_faces(encoding1, encoding2, tolerance)
                        comparison_result = {
                            "lawyer1_id": lawyer1.get('id'),
                            "lawyer1_oab": lawyer1.get('oab_id'),
                            "lawyer2_id": lawyer2.get('id'),
                            "lawyer2_oab": lawyer2.get('oab_id'),
                            "is_match": is_match,
                            "distance": round(distance, 4),
                            "confidence": round((1 - distance) * 100, 2) if distance <= 1 else 0
                        }
                    
                    results["comparisons"].append(comparison_result)
            
            # Group similar lawyers
            results["groups"] = self.group_similar_lawyers(lawyers_data, lawyer_encodings, tolerance)
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing JSON file: {str(e)}")
            return {"error": str(e)}
    
    def group_similar_lawyers(self, lawyers_data: List[Dict], 
                            encodings: List[np.ndarray], 
                            tolerance: float) -> List[List[Dict]]:
        """
        Group lawyers that appear to be the same person
        
        Args:
            lawyers_data: List of lawyer dictionaries
            encodings: List of face encodings
            tolerance: Face comparison tolerance
            
        Returns:
            List of groups, where each group contains lawyers that match
        """
        groups = []
        processed = set()
        
        for i, lawyer1 in enumerate(lawyers_data):
            if i in processed or encodings[i] is None:
                continue
            
            current_group = [lawyer1]
            processed.add(i)
            
            for j, lawyer2 in enumerate(lawyers_data):
                if j <= i or j in processed or encodings[j] is None:
                    continue
                
                is_match, _ = self.compare_faces(encodings[i], encodings[j], tolerance)
                
                if is_match:
                    current_group.append(lawyer2)
                    processed.add(j)
            
            if len(current_group) > 1:
                groups.append(current_group)
        
        return groups

def main():
    """
    Main function to run the face comparison
    """
    # Initialize the comparator
    comparator = LawyerFaceComparator()
    
    # Example usage
    json_file_path = "joao_augusto_da_silva_lawyers.json"  # Your JSON file path
    
    # Process the lawyers
    results = comparator.process_lawyers_json(json_file_path, tolerance=0.6)
    
    # Print results
    print(json.dumps(results, indent=2, ensure_ascii=False))
    
    # Save results to file
    output_file = json_file_path.replace('.json', '_comparison_results.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nResults saved to: {output_file}")

if __name__ == "__main__":
    main()
