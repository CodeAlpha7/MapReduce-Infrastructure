# CMakeLists.txt
cmake_minimum_required(VERSION 3.10)

project(project4)

set (CMAKE_CXX_FLAGS "-lstdc++fs -std=c++17")

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Threads REQUIRED)

add_subdirectory(external)
add_subdirectory(src)
add_subdirectory(test)
