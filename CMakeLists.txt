cmake_minimum_required(VERSION 3.16)

project(playground)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY playground/)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_subdirectory(src)
add_subdirectory(tests)
