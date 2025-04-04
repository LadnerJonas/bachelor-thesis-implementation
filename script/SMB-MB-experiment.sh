#!/bin/bash

# File paths
FILES=(
    "include/smb/worker/process_morsel_smb_batched.hpp"
    "include/smb/worker/process_morsel_smb_lock_free_batched.hpp"
    "include/cmp/worker/CmpProcessor.hpp"
    "include/cmp/worker/CmpProcessorOfUnit.hpp"
    "include/hybrid/worker/request_and_process_chunk.hpp"
    "include/lpam/worker/process_morsel_lpam.hpp"
)

# Target values in KB and MB
#256 512 1024
VALUES=(2048 4096 8192 16384) # 256KiB to 16MiB

# Build and benchmark command
BUILD_CMD="cmake --build --preset release-build"
BENCHMARK_CMD="build/release/benchmark/benchmark_shuffle"
BENCHMARK_OUTPUT_DIR="../benchmark-results/smb-experiments"

# Create the output directory if it doesn't exist
mkdir -p "$BENCHMARK_OUTPUT_DIR"

is_server=$(hostname | grep -q "atkemper4")
if [ "$is_server" ]; then
    file_name_prefix="server"
else
    file_name_prefix="laptop"
fi

# Output file for concatenated results
CONCATENATED_OUTPUT="${BENCHMARK_OUTPUT_DIR}/${file_name_prefix}-$(date +%Y-%m-%d)-benchmark_results.log"
> "$CONCATENATED_OUTPUT"

# Loop through the values
for VALUE in "${VALUES[@]}"; do
    # Format value for filenames (e.g., 128KiB, 1MiB)
    if [ "$VALUE" -lt 1024 ]; then
        SIZE_LABEL="${VALUE}KiB"
    else
        SIZE_LABEL="$((${VALUE} / 1024))MiB"
    fi

    echo "Updating buffer size to ${VALUE} KiB (${SIZE_LABEL})"

    # Update buffer size in all files
    for FILE in "${FILES[@]}"; do
        sed -i "s/buffer_base_value = [0-9]\+/buffer_base_value = ${VALUE}/g" "$FILE"
    done

    # Build the project
    echo "Building the project..."
    $BUILD_CMD || { echo "Build failed! Exiting."; exit 1; }

    # Run the benchmark
    OUTPUT_FILE="${BENCHMARK_OUTPUT_DIR}/${file_name_prefix}-$(date +%Y-%m-%d)-${SIZE_LABEL}-SMB-Scaling.log"
    echo "Running benchmark and saving results to ${OUTPUT_FILE}..."
    $BENCHMARK_CMD | tee "$OUTPUT_FILE"

    # Prepend size label to orchestrator names
    echo "Prepending size label to orchestrator names in ${OUTPUT_FILE}..."
    sed -i "s/^Smb/${SIZE_LABEL}-&/g" "$OUTPUT_FILE"
    sed -i "s/^CmpProcessor/${SIZE_LABEL}-&/g" "$OUTPUT_FILE"

    # Append the contents of the current benchmark file to the concatenated output file
    cat "$OUTPUT_FILE" >> "$CONCATENATED_OUTPUT"
done

echo "Script completed successfully."
