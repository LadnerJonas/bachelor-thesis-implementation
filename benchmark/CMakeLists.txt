include_directories(${CMAKE_SOURCE_DIR}/external/perfevent)

add_executable(benchmark_tuple-generator tuple-generator/benchmark.cpp)
add_executable(benchmark_shuffle shuffle/benchmark.cpp)
add_executable(benchmark_materialization materialization/benchmark.cpp)