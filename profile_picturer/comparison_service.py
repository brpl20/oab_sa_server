import cv2
import numpy as np
import boto3
import os
import tempfile
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import requests
from io import BytesIO
from PIL import Image
import face_recognition
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class LawyerComparison:
    lawyer1_id: int
    lawyer2_id: int
    similarity_score: float
    are_same_person: bool
    confidence: str

class LawyerFaceComparator:
    def __init__(self):
        # Initialize AWS S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        self.bucket_name = 'oabapi-profile-pic'
        
        # Thresholds for face comparison
        self.SAME_PERSON_THRESHOLD = 0.6  # Lower is more similar for face_recognition
        self.HIGH_CONFIDENCE_THRESHOLD = 0.4
        self.MEDIUM_CONFIDENCE_THRESHOLD = 0.7
    
    def download_image_from_s3(self, profile_picture_filename: str) -> Optional[np.ndarray]:
        """Download image from S3 and return as OpenCV image array"""
        try:
            # Download image from S3
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=profile_picture_filename
            )
            
            # Convert to PIL Image then to OpenCV format
            image_data = response['Body'].read()
            pil_image = Image.open(BytesIO(image_data))
            
            # Convert PIL to OpenCV format (RGB to BGR)
            opencv_image = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)
            
            logger.info(f"Successfully downloaded image: {profile_picture_filename}")
            return opencv_image
            
        except Exception as e:
            logger.error(f"Error downloading image {profile_picture_filename}: {str(e)}")
            return None
    
    def extract_face_encoding(self, image: np.ndarray) -> Optional[np.ndarray]:
        """Extract face encoding from image using face_recognition library"""
        try:
            # Convert BGR to RGB for face_recognition
            rgb_image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            
            # Find face locations
            face_locations = face_recognition.face_locations(rgb_image)
            
            if not face_locations:
                logger.warning("No face found in image")
                return None
            
            # Get face encodings (use the first face if multiple found)
            face_encodings = face_recognition.face_encodings(rgb_image, face_locations)
            
            if not face_encodings:
                logger.warning("Could not encode face in image")
                return None
            
            return face_encodings[0]
            
        except Exception as e:
            logger.error(f"Error extracting face encoding: {str(e)}")
            return None
    
    def compare_faces(self, encoding1: np.ndarray, encoding2: np.ndarray) -> Tuple[float, bool, str]:
        """Compare two face encodings and return similarity score and decision"""
        try:
            # Calculate face distance (lower = more similar)
            distance = face_recognition.face_distance([encoding1], encoding2)[0]
            
            # Determine if same person
            are_same_person = distance <= self.SAME_PERSON_THRESHOLD
            
            # Determine confidence level
            if distance <= self.HIGH_CONFIDENCE_THRESHOLD:
                confidence = "HIGH"
            elif distance <= self.MEDIUM_CONFIDENCE_THRESHOLD:
                confidence = "MEDIUM"
            else:
                confidence = "LOW"
            
            return distance, are_same_person, confidence
            
        except Exception as e:
            logger.error(f"Error comparing faces: {str(e)}")
            return 1.0, False, "ERROR"
    
    def compare_lawyers_by_name(self, full_name: str) -> List[LawyerComparison]:
        """Compare all lawyers with the same name and return comparison results"""
        # This would be called from your Rails app
        # For now, simulating the lawyer data structure
        
        # You would replace this with actual Rails model query:
        # lawyers = Lawyer.where(full_name: full_name)
        
        # For demonstration, using the example data you provided
        lawyers = self._get_example_lawyers(full_name)
        
        if len(lawyers) < 2:
            logger.info(f"Only {len(lawyers)} lawyer(s) found with name '{full_name}'. No comparison needed.")
            return []
        
        comparisons = []
        
        # Compare each lawyer with every other lawyer
        for i in range(len(lawyers)):
            for j in range(i + 1, len(lawyers)):
                lawyer1 = lawyers[i]
                lawyer2 = lawyers[j]
                
                logger.info(f"Comparing {lawyer1['oab_id']} vs {lawyer2['oab_id']}")
                
                # Download images
                image1 = self.download_image_from_s3(lawyer1['profile_picture'])
                image2 = self.download_image_from_s3(lawyer2['profile_picture'])
                
                if image1 is None or image2 is None:
                    logger.warning(f"Could not download images for comparison")
                    continue
                
                # Extract face encodings
                encoding1 = self.extract_face_encoding(image1)
                encoding2 = self.extract_face_encoding(image2)
                
                if encoding1 is None or encoding2 is None:
                    logger.warning(f"Could not extract face encodings for comparison")
                    continue
                
                # Compare faces
                distance, are_same_person, confidence = self.compare_faces(encoding1, encoding2)
                
                comparison = LawyerComparison(
                    lawyer1_id=lawyer1['id'],
                    lawyer2_id=lawyer2['id'],
                    similarity_score=distance,
                    are_same_person=are_same_person,
                    confidence=confidence
                )
                
                comparisons.append(comparison)
                
                logger.info(f"Comparison result: {lawyer1['oab_id']} vs {lawyer2['oab_id']} - "
                          f"Distance: {distance:.3f}, Same person: {are_same_person}, "
                          f"Confidence: {confidence}")
        
        return comparisons
    
    def _get_example_lawyers(self, full_name: str) -> List[Dict]:
        """Example lawyer data - replace this with actual Rails model query"""
        if full_name == "JOAO AUGUSTO DA SILVA":
            return [
                {
                    'id': 263159,
                    'full_name': 'JOAO AUGUSTO DA SILVA',
                    'oab_id': 'PR_11582',
                    'profile_picture': 'PR_11582_profile_pic.jpg',
                    'state': 'PR'
                },
                {
                    'id': 1622431,
                    'full_name': 'JOAO AUGUSTO DA SILVA',
                    'oab_id': 'GO_43255',
                    'profile_picture': 'GO_43255_profile_pic.jpg',
                    'state': 'GO'
                },
                {
                    'id': 64948,
                    'full_name': 'JOAO AUGUSTO DA SILVA',
                    'oab_id': 'MG_807',
                    'profile_picture': 'MG_807_profile_pic.jpg',
                    'state': 'MG'
                }
            ]
        return []
    
    def generate_report(self, comparisons: List[LawyerComparison]) -> str:
        """Generate a human-readable report of the comparisons"""
        if not comparisons:
            return "No comparisons performed."
        
        report = f"Face Comparison Report\n{'='*50}\n\n"
        
        same_person_pairs = []
        different_person_pairs = []
        
        for comp in comparisons:
            if comp.are_same_person:
                same_person_pairs.append(comp)
            else:
                different_person_pairs.append(comp)
        
        if same_person_pairs:
            report += "LIKELY SAME PERSON:\n"
            report += "-" * 20 + "\n"
            for comp in same_person_pairs:
                report += f"Lawyer ID {comp.lawyer1_id} ↔ Lawyer ID {comp.lawyer2_id}\n"
                report += f"  Similarity Score: {comp.similarity_score:.3f}\n"
                report += f"  Confidence: {comp.confidence}\n\n"
        
        if different_person_pairs:
            report += "LIKELY DIFFERENT PERSONS:\n"
            report += "-" * 25 + "\n"
            for comp in different_person_pairs:
                report += f"Lawyer ID {comp.lawyer1_id} ↔ Lawyer ID {comp.lawyer2_id}\n"
                report += f"  Similarity Score: {comp.similarity_score:.3f}\n"
                report += f"  Confidence: {comp.confidence}\n\n"
        
        return report

# Usage example
def main():
    """Example usage of the LawyerFaceComparator"""
    comparator = LawyerFaceComparator()
    
    # Compare all lawyers named "JOAO AUGUSTO DA SILVA"
    comparisons = comparator.compare_lawyers_by_name("JOAO AUGUSTO DA SILVA")
    
    # Generate and print report
    report = comparator.generate_report(comparisons)
    print(report)
    
    return comparisons

if __name__ == "__main__":
    main()