#!/bin/bash

# Autonomous OAB Batch Runner Script for Server Environments
# This script processes lawyer files in batches, managing concurrency and logging.

# --- Configuration ---
# IMPORTANT: Adjust these paths and limits according to your server environment and resources.

# Base path where your input lawyer JSON files are located.
# Example: /home/ubuntu/oab_scraper/data/input
BASE_PATH="/home/ubuntu/oab_scraper/data/input"

# Name of your Python scraping script.
# Make sure this script is in the same directory as this Bash runner script, 
# or provide the full path if it's elsewhere.
PYTHON_SCRIPT="request_lawyers_with_society_retry_errorr_with_delay.py" 

# Path to your Python virtual environment activation script.
# Example: /home/ubuntu/oab_scraper/oabsa_env/bin/activate
VENV_PATH="/home/ubuntu/oab_scraper/oabsa_env/bin/activate" 

# Range of lawyer part files to process lawyers_001.json to lawyers_200.json
START_PART=001
END_PART=025



# Maximum number of Python scraping processes to run concurrently.
# Start with a low number (e.g., 2-5) and increase gradually based on server resources (CPU, RAM)
# and proxy provider limits. 100 concurrent Selenium instances are VERY resource-intensive.
MAX_CONCURRENT_PROCESSES=25 # Recommended to start low. Adjust as needed.

# Delay in seconds between launching new batches, if slots are available.
DELAY_BETWEEN_LAUNCHES=10

# Delay in seconds when waiting for a process slot to become free.
WAIT_FOR_SLOT_DELAY=10

# --- Colors for output (for better readability in console) ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- Logging Directory ---
LOG_DIR="batch_logs"
MASTER_LOG="runner.log"
COMPLETED_LOG="completed_batches.log"
FAILED_LOG="failed_batches.log"
MISSING_LOG="missing_files.log"

# --- Helper Functions ---

# Function to print colored status messages
print_status() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$MASTER_LOG"
}

# Function to print colored success messages
print_success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] ✅ $1${NC}" | tee -a "$MASTER_LOG"
}

# Function to print colored error messages
print_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ❌ $1${NC}" | tee -a "$MASTER_LOG"
}

# Function to print colored warning messages
print_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] ⚠️  $1${NC}" | tee -a "$MASTER_LOG"
}

# Function to check if a file exists
check_file_exists() {
    local file_path="$1"
    if [[ -f "$file_path" ]]; then
        return 0 # File exists
    else
        return 1 # File does not exist
    fi
}

# Function to get the count of currently running Python scraper processes
get_running_processes_count() {
    # We use 'pgrep -f' to search for the full command line of our Python script.
    # 'wc -l' counts the lines (PIDs), 'tr -d ' ' removes whitespace.
    # We exclude the 'pgrep' command itself from the count.
    pgrep -f "python3 ${PYTHON_SCRIPT}" | grep -v "pgrep" | wc -l | tr -d ' '
}

# --- Main Execution Logic ---
main() {
    print_status "🚀 STARTING AUTONOMOUS OAB BATCH RUNNER"
    print_status "📂 Base path for input files: $BASE_PATH"
    print_status "🐍 Python scraping script: $PYTHON_SCRIPT"
    print_status "🌍 Virtual environment path: $VENV_PATH"
    print_status "📋 Processing parts from $START_PART to $END_PART"
    print_status "⚙️  Max concurrent processes: $MAX_CONCURRENT_PROCESSES"
    print_status "📝 All logs will be in: $LOG_DIR/"
    echo ""
    
    # Create log directory if it doesn't exist
    mkdir -p "$LOG_DIR"
    
    # Initialize log files
    echo "# Autonomous OAB Runner - Started at $(date)" > "$MASTER_LOG"
    echo "# Completed batches" > "$COMPLETED_LOG"
    echo "# Failed batches" > "$FAILED_LOG"
    echo "# Missing files" > "$MISSING_LOG"
    
    # Check if virtual environment activation script exists
    if ! check_file_exists "$VENV_PATH"; then
        print_error "Virtual environment activation script not found: $VENV_PATH"
        print_error "Please verify the VENV_PATH configuration."
        exit 1
    fi
    
    # Check if Python script exists
    if ! check_file_exists "$PYTHON_SCRIPT"; then
        print_error "Python script not found: $PYTHON_SCRIPT"
        print_error "Please ensure '$PYTHON_SCRIPT' is in the current directory or provide full path."
        exit 1
    fi

    local total_batches_attempted=0
    local batches_skipped_missing_file=0
    local pids=() # Array to store PIDs of running background processes

    for part in $(seq $START_PART $END_PART); do
        echo "" # Newline for better separation in console output
        print_status "================================================================================"
        print_status "📦 STARTING BATCH $part (File part: $(printf "%03d" $part))"
        print_status "================================================================================"
        
        local formatted_part=$(printf "%03d" $part)
        local input_file="${BASE_PATH}/lawyers_part_${formatted_part}.json"
        local batch_log_file="${LOG_DIR}/batch_${formatted_part}_$(date +%Y%m%d%H%M%S).log"
        
        # Check if the input file exists
        if ! check_file_exists "$input_file"; then
            print_warning "Input file not found: $input_file. Skipping batch $part."
            echo "$part" >> "$MISSING_LOG"
            batches_skipped_missing_file=$((batches_skipped_missing_file + 1))
            continue # Move to the next part number
        fi

        # --- Concurrency Control ---
        while true; do
            local current_running=$(get_running_processes_count)
            print_status "🔍 Current running processes: $current_running / $MAX_CONCURRENT_PROCESSES"

            if [[ "$current_running" -lt "$MAX_CONCURRENT_PROCESSES" ]]; then
                break # Slot available, exit loop and launch new process
            else
                print_status "🚫 Max concurrent processes reached. Waiting for a slot to free up..."
                # Use wait -n (if available, Bash 4.3+) to wait for any background job to finish.
                # Otherwise, sleep and re-check.
                if command -v wait &>/dev/null && wait -n 2>/dev/null; then
                    print_status "✅ A process finished, slot available."
                else
                    sleep "$WAIT_FOR_SLOT_DELAY"
                fi
            fi
        done
        # --- End Concurrency Control ---

        total_batches_attempted=$((total_batches_attempted + 1))
        
        print_status "🚀 Launching Python script for batch $part (input: ${input_file}). Output to: ${batch_log_file}"

        # Run the Python script in the background using nohup to detach it from the terminal.
        # Redirect stdout and stderr to the batch-specific log file.
        nohup bash -c "source \"$VENV_PATH\" && python3 \"$PYTHON_SCRIPT\" \"$input_file\"" > "$batch_log_file" 2>&1 &
        
        local current_pid=$! # Get the PID of the last background command
        pids+=($current_pid) # Add PID to our array for tracking

        print_status "✨ Launched batch $part with PID: $current_pid"

        # Brief delay between launching processes to avoid hammering the system
        sleep "$DELAY_BETWEEN_LAUNCHES"
    done # End of main loop for parts

    echo ""
    print_status "================================================================================"
    print_status "⏳ All batches launched. Waiting for remaining processes to complete..."
    print_status "================================================================================"
    
    # Wait for all launched background processes to complete
    for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then # Check if process is still running
            print_status "Waiting for PID $pid to finish..."
            wait "$pid"
            local exit_status=$? # Get the exit status of the waited process
            if [[ "$exit_status" -eq 0 ]]; then
                print_success "PID $pid (Batch corresponding) finished successfully."
                # Note: We can't easily link PID back to file part here without more complex tracking.
                # The success/failure for individual batches will be visible in their specific log files.
            else
                print_error "PID $pid (Batch corresponding) exited with status $exit_status (likely failed)."
            fi
        fi
    done
    
    # A final check for any lingering processes
    local final_running_count=$(get_running_processes_count)
    if [[ "$final_running_count" -gt 0 ]]; then
        print_warning "Some Python processes might still be running after explicit waits. Please check manually."
        print_warning "Use 'pgrep -f \"python3 ${PYTHON_SCRIPT}\"' to find them."
    else
        print_success "All Python scraper processes have finished."
    fi

    echo ""
    print_status "================================================================================"
    print_status "🎉 AUTONOMOUS RUNNER COMPLETED SUMMARY"
    print_status "================================================================================"
    print_status "Total batches attempted to launch: $total_batches_attempted"
    print_status "Batches skipped (file missing): $batches_skipped_missing_file"
    echo ""
    print_status "📝 Check these logs for details:"
    print_status "   - ${MASTER_LOG} (Overall runner activity)"
    print_status "   - ${LOG_DIR}/batch_*.log (Detailed logs for each batch)"
    print_status "   - ${COMPLETED_LOG} (You'd need to manually parse batch_*.log for this logic now)"
    print_status "   - ${FAILED_LOG} (You'd need to manually parse batch_*.log for this logic now)"
    print_status "   - ${MISSING_LOG} (List of files not found)"
    echo ""
    print_status "Tip: Use 'tail -f ${LOG_DIR}/batch_XXX_*.log' for specific batch monitoring."
    print_status "Tip: Use 'grep -L 'Concluído' ${LOG_DIR}/batch_*.log' to find logs of potentially failed batches."
}

# --- Trap to handle script interruption ---
# If Ctrl+C is pressed, it will try to kill child processes and exit gracefully.
trap '
    print_warning "Script interrupted! Attempting to gracefully stop child processes..."
    # Kill all child processes of the current script, which are the nohup python processes
    pkill -TERM -P $$ || true # Send TERM signal (allow graceful exit)
    sleep 5 # Give them time to shut down
    pkill -KILL -P $$ || true # If still running, force kill
    print_error "Runner script terminated prematurely."
    exit 1
' INT TERM

# Run the main function
main "$@"
