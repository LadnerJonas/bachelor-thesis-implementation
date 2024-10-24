cmake_minimum_required(VERSION 3.20.4)
project(prototype)

set(CMAKE_CXX_STANDARD 23)

set(CMAKE_CXX_FLAGS "-Wall -Wextra -Wno-interference-size")
#set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fsanitize=address,undefined -D_GLIBCXX_DEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG")

include_directories(include)

add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(benchmark)

# Add a custom target to format all source files with clang-format
add_custom_target(format
        COMMAND git diff --name-only -- '*.cpp' '*.hpp' > modified_files.txt
        COMMAND cat modified_files.txt
        COMMAND clang-format -i `cat modified_files.txt`

        COMMAND rm modified_files.txt
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Running clang-format on modified .cpp and .hpp files"
)

# Custom target to run clang-tidy on the modified files
add_custom_target(tidy
        COMMAND git diff --name-only -- '*.cpp' '*.hpp' > modified_files.txt
        COMMAND cat modified_files.txt
        COMMAND clang-tidy -fix -fix-errors `cat modified_files.txt` -- -I${CMAKE_SOURCE_DIR}/include
        COMMAND rm modified_files.txt
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Running clang-tidy on modified .cpp and .hpp files"
)

add_custom_target(clang_format_tidy
        COMMAND ${CMAKE_COMMAND} --build . --target format
        COMMAND ${CMAKE_COMMAND} --build . --target tidy
        COMMENT "Running clang-format and clang-tidy on modified .cpp and .hpp files"
)