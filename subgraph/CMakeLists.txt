cmake_minimum_required(VERSION 3.16)
project(SubgraphMatching CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

include_directories(include)

find_package(JNI REQUIRED)

# SHARED = 动态库 (.dll on Windows, .so on Linux)
add_library(subgraph_matching SHARED
    src/subgraph_jni.cpp
)

# target_include_directories 必须在 add_executable 之后
target_include_directories(subgraph_matching PRIVATE
    ${JNI_INCLUDE_DIRS}
)

if(MSVC)
    target_compile_options(subgraph_matching PRIVATE
        $<$<CONFIG:Release>:/O2>
        /W4
        /WX-
        /utf-8
    )
else()
    target_compile_options(subgraph_matching PRIVATE
        $<$<CONFIG:Release>:-O3 -march=native>
        -Wall
        -Wextra
    )
endif()