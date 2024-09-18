cmake_minimum_required(VERSION 3.30)
project(Implementierung)

set(CMAKE_CXX_STANDARD 23)

include_directories(${PROJECT_SOURCE_DIR}/include)


#set(CMAKE_CXX_CLANG_TIDY "clang-tidy;-checks=*")

add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(benchmark)


set(CMAKE_CXX_FLAGS "-Wall -Wextra -Wno-interference-size")
#set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fsanitize=address,undefined -D_GLIBCXX_DEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-O1 ${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native ${CMAKE_CXX_FLAGS}")


# Add a custom target to format all source files with clang-format
add_custom_target(format
        COMMAND find ${CMAKE_SOURCE_DIR} -name "*.cpp" -o -name "*.hpp" | xargs clang-format -i
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Running clang-format to reorder includes and format code"
)