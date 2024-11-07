
add_executable(src main.cpp)
target_include_directories(src PRIVATE ../include)
find_package(Threads REQUIRED)
target_link_libraries(src PRIVATE Threads::Threads)

find_package(TBB REQUIRED)
target_link_libraries(src PRIVATE TBB::tbb)