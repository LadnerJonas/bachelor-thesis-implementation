cmake_minimum_required(VERSION 3.20.4)
project(prototype)

set(CMAKE_CXX_STANDARD 23)

set(CMAKE_CXX_FLAGS "-Wall -Wextra -Wno-interference-size")
#set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fsanitize=address,undefined -D_GLIBCXX_DEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native")

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

# init input data
# Path to the virtual environment's Python interpreter
set(PYTHON_EXECUTABLE ${CMAKE_SOURCE_DIR}/../venv/bin/python)

# Define a custom target to run the create_input_data.py script
add_custom_target(create_input_data ALL
        COMMAND ${PYTHON_EXECUTABLE} create_input_data.py
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/input_data
        COMMENT "Running input_data/create_input_data.py script"
)