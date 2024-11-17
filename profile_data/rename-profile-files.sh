#!/bin/bash

# Directory containing the .gcda files with "cmake-build-pgo_instrument" in their names
TARGET_DIR="profile_data"

# Find and rename all files with "cmake-build-pgo_instrument" in the filename
find "$TARGET_DIR" -type f -name "*cmake-build-pgo_instrument*" | while IFS= read -r file; do
    # Create the new filename by replacing "cmake-build-pgo_instrument" with "cmake-build-pgo_optimize"
    new_file="${file//cmake-build-pgo_instrument/cmake-build-pgo_optimize}"
    
    # Rename the file
    mv "$file" "$new_file"
    echo "Renamed $file to $new_file"
done

echo "Renaming complete. All files have been updated."
