#!/bin/bash

# OAB Server Initialization and Batch Runner Script
# This script sets up the environment, syncs data from S3, and processes lawyer files

# --- Configuration ---
BASE_PATH="/home/ubuntu/oab_scraper"
DATA_INPUT_PATH="$BASE_PATH/data/input"
REPO_URL="git@github.com:brpl20/oab_sa_server.git"
S3_BUCKET="oab-jsons-sa2"
S3_REGION="us-east-1"
PYTHON_SCRIPT="request_lawyers_with_society_retry_errorr_with_delay.py"
VENV_PATH="$BASE_PATH/oabsa_env/bin/activate"

# File range configuration
START_PART=001
END_PART=025
MAX_CONCURRENT_PROCESSES=25
DELAY_BETWEEN_LAUNCHES=10
WAIT_FOR_SLOT_DELAY=10

# --- Colors for output ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# --- Logging ---
LOG_DIR="$BASE_PATH/batch_logs"
MASTER_LOG="$BASE_PATH/runner.log"
SETUP_LOG="$BASE_PATH/setup.log"

# --- Helper Functions ---

print_status() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$SETUP_LOG"
}

print_success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] ✅ $1${NC}" | tee -a "$SETUP_LOG"
}

print_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ❌ $1${NC}" | tee -a "$SETUP_LOG"
}

print_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] ⚠️  $1${NC}" | tee -a "$SETUP_LOG"
}

print_header() {
    echo -e "${PURPLE}[$(date '+%Y-%m-%d %H:%M:%S')] 🚀 $1${NC}" | tee -a "$SETUP_LOG"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to load environment variables
load_env() {
    local env_file="/home/ubuntu/.env"
    if [[ -f "$env_file" ]]; then
        print_status "Loading environment variables from $env_file"
        set -a  # automatically export all variables
        source "$env_file"
        set +a
        print_success "Environment variables loaded from /home/ubuntu/.env"
    else
        print_error ".env file not found at $env_file"
        print_error "Please create .env file at /home/ubuntu/.env with required credentials:"
        echo "AWS_ACCESS_KEY_ID=your_access_key"
        echo "AWS_SECRET_ACCESS_KEY=your_secret_key"
        echo "AWS_BUCKET=oab-jsons-sa2"
        echo "AWS_DEFAULT_REGION=us-east-1"
        echo "GH_TOKEN=your_github_token"
        echo "PROXY_USERNAME=your_proxy_username"
        echo "PROXY_PASSWORD=your_proxy_password"
        echo "PROXY_HOST=your_proxy_host:port"
        exit 1
    fi
}

# Function to setup AWS CLI
setup_aws_cli() {
    print_status "Setting up AWS CLI configuration..."
    
    if ! command_exists aws; then
        print_status "AWS CLI not found. Installing..."
        
        # Create temporary directory
        local temp_dir=$(mktemp -d)
        cd "$temp_dir"
        
        print_status "Downloading AWS CLI installer..."
        if ! curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"; then
            print_error "Failed to download AWS CLI installer"
            exit 1
        fi
        
        print_status "Extracting AWS CLI installer..."
        if ! unzip -q awscliv2.zip; then
            print_error "Failed to extract AWS CLI installer"
            exit 1
        fi
        
        print_status "Installing AWS CLI (this may take a moment)..."
        if sudo ./aws/install; then
            print_success "AWS CLI installed successfully"
        else
            print_error "Failed to install AWS CLI"
            exit 1
        fi
        
        # Clean up
        cd "$BASE_PATH"
        rm -rf "$temp_dir"
        
        # Verify installation
        sleep 2
        if command_exists aws; then
            print_success "AWS CLI installation verified"
        else
            print_error "AWS CLI installation failed - command not found after install"
            exit 1
        fi
    else
        print_status "AWS CLI already installed"
    fi
    
    # Configure AWS CLI with credentials from environment
    print_status "Configuring AWS CLI credentials..."
    aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID"
    aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY"
    aws configure set default.region "$AWS_DEFAULT_REGION"
    aws configure set default.output json
    
    print_status "Testing AWS S3 connection..."
    if aws s3 ls "s3://$S3_BUCKET" >/dev/null 2>&1; then
        print_success "AWS S3 connection successful - bucket accessible"
    else
        print_error "Failed to connect to S3 bucket: $S3_BUCKET"
        print_error "Please check your AWS credentials and bucket name"
        exit 1
    fi
}

# Function to create directory structure
setup_directories() {
    print_status "Setting up directory structure..."
    
    mkdir -p "$BASE_PATH"
    mkdir -p "$DATA_INPUT_PATH"
    mkdir -p "$LOG_DIR"
    
    print_success "Directories created:
    - $BASE_PATH
    - $DATA_INPUT_PATH  
    - $LOG_DIR"
}

# Function to clone repository
clone_repository() {
    print_status "Cloning repository with GitHub token..."
    
    cd "$BASE_PATH"
    
    if [[ -z "$GH_TOKEN" ]]; then
        print_error "GH_TOKEN not found in environment variables"
        print_error "Please add GH_TOKEN=your_github_token to your .env file"
        exit 1
    fi
    
    # Construct authenticated HTTPS URL
    local repo_url="https://${GH_TOKEN}@github.com/brpl20/oab_sa_server.git"
    
    if [[ -d "oab_sa_server" ]]; then
        print_status "Repository already exists, pulling latest changes..."
        cd oab_sa_server
        
        # Set the remote URL with token for pulling
        git remote set-url origin "$repo_url"
        
        if git pull origin main; then
            print_success "Repository updated successfully"
        elif git pull origin master; then
            print_success "Repository updated successfully (master branch)"
        else
            print_error "Failed to update repository"
            exit 1
        fi
        cd ..
    else
        print_status "Cloning fresh repository..."
        if git clone "$repo_url"; then
            print_success "Repository cloned successfully"
        else
            print_error "Failed to clone repository. Please check your GH_TOKEN"
            exit 1
        fi
    fi
    
    # Wait for repository folder to appear and be accessible
    print_status "Waiting for repository folder to be fully available..."
    local max_wait=30
    local wait_count=0
    while [[ ! -d "oab_sa_server" ]] && [[ $wait_count -lt $max_wait ]]; do
        sleep 1
        wait_count=$((wait_count + 1))
        print_status "Waiting for repo folder... ($wait_count/$max_wait)"
    done
    
    if [[ ! -d "oab_sa_server" ]]; then
        print_error "Repository folder did not appear after $max_wait seconds"
        exit 1
    fi
    
    print_success "Repository folder confirmed available"
    
    # Copy .env file from /home/ubuntu to repository folder
    print_status "Copying .env file to repository folder..."
    local source_env="/home/ubuntu/.env"
    local dest_env="$BASE_PATH/oab_sa_server/.env"
    
    if [[ -f "$source_env" ]]; then
        if cp "$source_env" "$dest_env"; then
            print_success "✅ .env file copied from /home/ubuntu/.env to $dest_env"
            # Verify the copy
            if [[ -f "$dest_env" ]]; then
                print_success "✅ .env file confirmed in repository folder"
            else
                print_error "❌ .env file copy verification failed"
            fi
        else
            print_error "❌ Failed to copy .env file to repository folder"
            exit 1
        fi
    else
        print_error "❌ Source .env file not found at $source_env"
        exit 1
    fi
}

# Function to sync files from S3
sync_from_s3() {
    print_status "Syncing lawyer files from S3..."
    
    print_status "Downloading files lawyers_001.json to lawyers_025.json from S3..."
    
    local downloaded_count=0
    local failed_count=0
    
    for i in $(seq -f "%03g" $START_PART $END_PART); do
        local s3_key="input/lawyers_${i}.json"
        local local_file="$DATA_INPUT_PATH/lawyers_${i}.json"
        
        print_status "Downloading s3://$S3_BUCKET/$s3_key -> $local_file"
        
        if aws s3 cp "s3://$S3_BUCKET/$s3_key" "$local_file"; then
            print_success "Downloaded lawyers_${i}.json"
            downloaded_count=$((downloaded_count + 1))
        else
            print_error "Failed to download lawyers_${i}.json"
            failed_count=$((failed_count + 1))
        fi
    done
    
    print_status "S3 Sync Summary:"
    print_success "Successfully downloaded: $downloaded_count files"
    if [[ $failed_count -gt 0 ]]; then
        print_warning "Failed downloads: $failed_count files"
    fi
    
    # List downloaded files
    print_status "Files in input directory:"
    ls -la "$DATA_INPUT_PATH/" | tee -a "$SETUP_LOG"
}

# Function to setup Python environment
setup_python_env() {
    print_status "Setting up Python environment..."
    
    # Check if virtual environment exists
    if [[ ! -f "$VENV_PATH" ]]; then
        print_status "Creating Python virtual environment..."
        python3 -m venv "$BASE_PATH/oabsa_env"
        print_success "Virtual environment created"
    else
        print_status "Virtual environment already exists"
    fi
    
    # Activate and install requirements
    print_status "Installing Python dependencies..."
    source "$VENV_PATH"
    
    # Install required packages
    pip install --upgrade pip
    pip install selenium requests beautifulsoup4 boto3 python-dotenv webdriver-manager
    
    print_success "Python environment setup complete"
}

# Function to copy scripts from repository
copy_scripts() {
    print_status "Copying scripts from repository..."
    
    if [[ -f "oab_sa_server/$PYTHON_SCRIPT" ]]; then
        cp "oab_sa_server/$PYTHON_SCRIPT" "$BASE_PATH/"
        print_success "Python script copied: $PYTHON_SCRIPT"
    else
        print_warning "Python script not found in repository: $PYTHON_SCRIPT"
        print_status "Looking for similar scripts..."
        find oab_sa_server/ -name "*.py" -type f | head -10 | tee -a "$SETUP_LOG"
    fi
    
    # Copy any additional configuration files
    if [[ -f "oab_sa_server/requirements.txt" ]]; then
        cp "oab_sa_server/requirements.txt" "$BASE_PATH/"
        print_success "Requirements file copied"
    fi
}

# Function to get running processes count
get_running_processes_count() {
    pgrep -f "python3 ${PYTHON_SCRIPT}" | grep -v "pgrep" | wc -l | tr -d ' '
}

# Function to run the batch processing
run_batch_processing() {
    print_header "STARTING BATCH PROCESSING"
    print_status "Each batch will process ONE file: lawyers_XXX.json"
    print_status "Total files to process: lawyers_$(printf "%03d" $START_PART).json to lawyers_$(printf "%03d" $END_PART).json"
    
    cd "$BASE_PATH"
    
    local total_batches_attempted=0
    local batches_skipped_missing_file=0
    local pids=()
    
    for part in $(seq $START_PART $END_PART); do
        print_status "================================================================================"
        print_status "📦 STARTING BATCH $part - Processing file: lawyers_$(printf "%03d" $part).json"
        print_status "================================================================================"
        
        local formatted_part=$(printf "%03d" $part)
        local input_file="${DATA_INPUT_PATH}/lawyers_${formatted_part}.json"
        local batch_log_file="${LOG_DIR}/batch_${formatted_part}_$(date +%Y%m%d%H%M%S).log"
        
        # Check if file exists
        if [[ ! -f "$input_file" ]]; then
            print_warning "Input file not found: $input_file. Skipping batch $part."
            echo "Missing file: lawyers_${formatted_part}.json" >> "${LOG_DIR}/missing_files.log"
            batches_skipped_missing_file=$((batches_skipped_missing_file + 1))
            continue
        fi
        
        # Show file info
        local file_size=$(du -h "$input_file" | cut -f1)
        print_status "📄 File: $input_file (Size: $file_size)"
        
        # Wait for available slot
        while true; do
            local current_running=$(get_running_processes_count)
            print_status "🔍 Current running processes: $current_running / $MAX_CONCURRENT_PROCESSES"
            
            if [[ "$current_running" -lt "$MAX_CONCURRENT_PROCESSES" ]]; then
                break
            else
                print_status "🚫 Max concurrent processes reached. Waiting for a slot..."
                sleep "$WAIT_FOR_SLOT_DELAY"
            fi
        done
        
        total_batches_attempted=$((total_batches_attempted + 1))
        
        print_status "🚀 Launching Python script for lawyers_${formatted_part}.json"
        print_status "📝 Log file: $batch_log_file"
        
        # Launch process in background - ONE FILE PER PROCESS
        nohup bash -c "source \"$VENV_PATH\" && python3 \"$PYTHON_SCRIPT\" \"$input_file\"" > "$batch_log_file" 2>&1 &
        
        local current_pid=$!
        pids+=($current_pid)
        
        print_success "✨ Launched batch $part with PID: $current_pid (Processing: lawyers_${formatted_part}.json)"
        sleep "$DELAY_BETWEEN_LAUNCHES"
    done
    
    print_status "================================================================================"
    print_status "⏳ All batches launched. Waiting for processes to complete..."
    print_status "   Total processes started: ${#pids[@]}"
    print_status "   Files being processed: $total_batches_attempted"
    print_status "   Files skipped (missing): $batches_skipped_missing_file"
    print_status "================================================================================"
    
    # Wait for all processes
    local completed_count=0
    for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            print_status "⏳ Waiting for PID $pid to finish... ($((completed_count + 1))/${#pids[@]})"
            wait "$pid"
            local exit_status=$?
            completed_count=$((completed_count + 1))
            if [[ "$exit_status" -eq 0 ]]; then
                print_success "✅ PID $pid finished successfully ($completed_count/${#pids[@]} completed)"
            else
                print_error "❌ PID $pid exited with status $exit_status ($completed_count/${#pids[@]} completed)"
            fi
        else
            print_status "PID $pid already finished"
            completed_count=$((completed_count + 1))
        fi
    done
    
    print_success "🎉 All batch processing completed!"
    print_status "📊 FINAL SUMMARY:"
    print_status "   • Total batches attempted: $total_batches_attempted"
    print_status "   • Files processed: $total_batches_attempted individual lawyer files"
    print_status "   • Files skipped (missing): $batches_skipped_missing_file"
    print_status "   • Each batch processed exactly ONE file: lawyers_XXX.json"
    print_status ""
    print_status "📂 Check logs in: $LOG_DIR/"
    print_status "   • Each batch_XXX_*.log contains the processing results for one file"
}

# Function to display help
show_help() {
    echo "OAB Server Initialization and Batch Runner"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  setup     - Full setup: clone repo, sync S3, setup environment"
    echo "  sync      - Sync files from S3 only"
    echo "  run       - Run batch processing only"
    echo "  full      - Complete setup and run (default)"
    echo "  help      - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 setup   # Just setup environment"
    echo "  $0 sync    # Just sync files from S3"
    echo "  $0 run     # Just run processing"
    echo "  $0 full    # Full setup and run"
}

# Main execution function
main() {
    local command=${1:-full}
    
    print_header "OAB SERVER INITIALIZATION STARTING"
    print_status "Command: $command"
    print_status "Base path: $BASE_PATH"
    print_status "S3 Bucket: $S3_BUCKET"
    print_status "Processing files: lawyers_$(printf "%03d" $START_PART).json to lawyers_$(printf "%03d" $END_PART).json"
    print_status "Each batch will process EXACTLY ONE file"
    
    # Initialize log
    echo "# OAB Server Setup - Started at $(date)" > "$SETUP_LOG"
    
    case $command in
        "setup")
            setup_directories
            load_env
            setup_aws_cli
            clone_repository
            sync_from_s3
            setup_python_env
            copy_scripts
            print_success "Setup completed! Run '$0 run' to start processing."
            ;;
        "sync")
            load_env
            setup_aws_cli
            sync_from_s3
            print_success "S3 sync completed!"
            ;;
        "run")
            if [[ ! -f "$VENV_PATH" ]]; then
                print_error "Python environment not found. Run '$0 setup' first."
                exit 1
            fi
            run_batch_processing
            ;;
        "full")
            setup_directories
            load_env
            setup_aws_cli
            clone_repository
            sync_from_s3
            setup_python_env
            copy_scripts
            print_success "Setup completed! Starting batch processing..."
            sleep 3
            run_batch_processing
            ;;
        "help"|"-h"|"--help")
            show_help
            ;;
        *)
            print_error "Unknown command: $command"
            show_help
            exit 1
            ;;
    esac
}

# Trap for graceful shutdown
trap '
    print_warning "Script interrupted! Attempting to stop child processes..."
    pkill -TERM -P $$ || true
    sleep 5
    pkill -KILL -P $$ || true
    print_error "Script terminated."
    exit 1
' INT TERM

# Run main function
main "$@"