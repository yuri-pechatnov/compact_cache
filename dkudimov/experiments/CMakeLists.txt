cmake_minimum_required(VERSION 3.24)
project(experiments)
set(CMAKE_CXX_STANDARD 20)            # Enable c++14 standard

# Add main.cpp file of the project root directory as a source file
set(SOURCE_FILES main.cpp)

# Add executable target with source files listed in SOURCE_FILES variable
add_executable(experiments ${SOURCE_FILES})