#!/bin/bash

# File paths
FILES=(
    "include/smb/worker/process_morsel_smb.hpp"
    "include/smb/worker/process_morsel_smb_lock_free_batched.hpp"
)

# Target values in MB
VALUES=(32 64) #1 2 4

# Build and benchmark command
BUILD_CMD="cmake --build --preset release-build"
BENCHMARK_CMD="build/release/benchmark/benchmark_shuffle"
BENCHMARK_OUTPUT_DIR="../benchmark"

# Create the output directory if it doesn't exist
mkdir -p $BENCHMARK_OUTPUT_DIR

# Loop through the values
for VALUE in "${VALUES[@]}"; do
    # Format value for filenames (e.g., 01MB, 02MB)
    FORMATTED_VALUE=$(printf "%02dMB" "$VALUE")

    echo "Updating buffer size to ${VALUE}MB"

    # Update buffer size in all files
    for FILE in "${FILES[@]}"; do
        sed -i "s/[0-9]\+ \* 1024 \* 1024/${VALUE} \* 1024 \* 1024/g" "$FILE"
    done

    # Build the project
    echo "Building the project..."
    $BUILD_CMD || { echo "Build failed! Exiting."; exit 1; }

    # Run the benchmark
    OUTPUT_FILE="${BENCHMARK_OUTPUT_DIR}/$(date +%Y-%m-%d)-${FORMATTED_VALUE}-SMB-Scaling.txt"
    echo "Running benchmark and saving results to ${OUTPUT_FILE}..."
    $BENCHMARK_CMD | tee "$OUTPUT_FILE"
done

echo "Script completed successfully."
