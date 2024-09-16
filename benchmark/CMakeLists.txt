# Add the include directory for the external library (PerfEvent)
include_directories(${CMAKE_SOURCE_DIR}/external/perfevent)

# Define the benchmark executable
add_executable(benchmark benchmark.cpp)

target_link_libraries(benchmark PRIVATE src_lib)

set(CMAKE_CXX_FLAGS_DEBUG "-O1")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -flto -DNDEBUG")