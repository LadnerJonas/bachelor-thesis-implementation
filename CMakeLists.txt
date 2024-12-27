cmake_minimum_required(VERSION 3.20.4)
project(implementation)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-interference-size")
set(CMAKE_CXX_FLAGS_DEBUG "-Og -g -fno-omit-frame-pointer -fno-inline -fsanitize=thread,undefined -D_GLIBCXX_DEBUG")

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG -fno-exceptions")
set(CMAKE_CXX_FLAGS_PROFILE "-O3 -march=native -DNDEBUG -fno-exceptions -g -fno-omit-frame-pointer -fno-inline")

# PGO
# (Instrumentation phase)
set(PROFILE_DATA_DIR "${CMAKE_SOURCE_DIR}/profile_data")
set(CMAKE_CXX_FLAGS_PGO_INSTRUMENT "-O3 -march=native -fprofile-generate=${PROFILE_DATA_DIR} -DNDEBUG -fno-exceptions")
# (Optimization phase using collected profile data)
set(CMAKE_CXX_FLAGS_PGO_OPTIMIZE "-O3 -march=native -fprofile-use=${PROFILE_DATA_DIR} -fprofile-correction -DNDEBUG -fno-exceptions")

#lto
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)


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
        COMMAND clang-tidy --extra-arg=-std=c++23 -fix -fix-errors `cat modified_files.txt` -- -I${CMAKE_SOURCE_DIR}/include
        COMMAND rm modified_files.txt
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Running clang-tidy on modified .cpp and .hpp files"
)

add_custom_target(clang_format_tidy
        COMMAND ${CMAKE_COMMAND} --build . --target format
        COMMAND ${CMAKE_COMMAND} --build . --target tidy
        COMMENT "Running clang-format and clang-tidy on modified .cpp and .hpp files"
)

# Path to the renaming script
set(RENAME_SCRIPT "${CMAKE_SOURCE_DIR}/profile_data/rename-profile-files.sh")

add_custom_target(run_rename_script
        COMMAND ${CMAKE_COMMAND} -E echo "Running pre-build renaming script..."
        COMMAND bash ${RENAME_SCRIPT}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Renaming .gcda files to match PGO_OPTIMIZE directory structure"
)