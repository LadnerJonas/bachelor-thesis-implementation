# src/CMakeLists.txt
# Collect source files
set(SOURCES
        morsel-driven/worker/process_morsels.cpp
        main.cpp
        ../include/morsel-driven/output_data_storage/LockFreeList.hpp
        morsel-driven/output_data_storage/StdVectorBasedPartitionManager.cpp
        morsel-driven/output_data_storage/LockFreeListBasedPartitionManager.cpp
        ../include/util/padded/PaddedAtomic.hpp
        ../include/in-place_sort/sort_partition.hpp
)

add_library(src_lib STATIC ${SOURCES})

# Create the executable target
add_executable(src ${SOURCES})

# Link any libraries if needed (e.g., pthread on Unix systems)
# target_link_libraries(MyExecutable PRIVATE pthread)

# Optionally, set include directories for this target
target_include_directories(src PRIVATE ${PROJECT_SOURCE_DIR}/include)
