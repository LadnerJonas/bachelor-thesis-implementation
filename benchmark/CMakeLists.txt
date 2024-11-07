include_directories(${CMAKE_SOURCE_DIR}/external/perfevent)

add_executable(benchmark_tuple-generator tuple-generator/benchmark.cpp)
add_executable(benchmark_shuffle shuffle/benchmark.cpp)
add_executable(benchmark_materialization materialization/benchmark.cpp)

find_package(TBB REQUIRED)
target_link_libraries(benchmark_tuple-generator PRIVATE TBB::tbb)
target_link_libraries(benchmark_shuffle PRIVATE TBB::tbb)
target_link_libraries(benchmark_materialization PRIVATE TBB::tbb)