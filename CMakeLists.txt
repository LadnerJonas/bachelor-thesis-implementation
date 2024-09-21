cmake_minimum_required(VERSION 3.29.6)
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

# init input data
# Path to the virtual environment's Python interpreter
set(PYTHON_EXECUTABLE ${CMAKE_SOURCE_DIR}/venv/bin/python)

# Define a custom target to run the create_input_data.py script
add_custom_target(create_input_data ALL
        COMMAND ${PYTHON_EXECUTABLE} create_input_data.py
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/input_data
        COMMENT "Running input_data/create_input_data.py script"
)