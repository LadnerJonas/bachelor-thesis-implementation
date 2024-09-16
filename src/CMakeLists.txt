# src/CMakeLists.txt
# Collect source files
set(SOURCES
        worker/process_morsels.cpp
        main.cpp
        ../include/util/PaddedMutex.hpp
)

# Create the executable target
add_executable(MyExecutable ${SOURCES})

# Link any libraries if needed (e.g., pthread on Unix systems)
# target_link_libraries(MyExecutable PRIVATE pthread)

# Optionally, set include directories for this target
target_include_directories(MyExecutable PRIVATE ${PROJECT_SOURCE_DIR}/include)
