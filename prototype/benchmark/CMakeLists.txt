# Add the include directory for the external library (PerfEvent)
include_directories(${CMAKE_SOURCE_DIR}/external/perfevent)

find_package(TBB REQUIRED)

# Define the benchmark executable
add_executable(benchmark benchmark.cpp)

target_link_libraries(benchmark PRIVATE TBB::tbb)

target_link_libraries(benchmark PRIVATE src_lib)