cmake_minimum_required(VERSION 3.16)
project(SubgraphMatching CXX)
 
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
 
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
 
add_library(subgraph_compile_options INTERFACE)
target_include_directories(subgraph_compile_options INTERFACE include)
 
if(MSVC)
    target_compile_options(subgraph_compile_options INTERFACE
        $<$<CONFIG:Release>:/O2>
        /W4
        /WX-
        /utf-8
    )
else()
    target_compile_options(subgraph_compile_options INTERFACE
        $<$<CONFIG:Release>:-O3 -march=native>
        -Wall
        -Wextra
    )
endif()
 
add_executable(subgraph_main src/main.cpp)
target_link_libraries(subgraph_main PRIVATE subgraph_compile_options)
 
find_package(Java COMPONENTS Development QUIET)
find_path(JNI_INCLUDE_DIR   jni.h   HINTS ENV JAVA_HOME PATH_SUFFIXES include)
find_path(JNI_MD_INCLUDE_DIR jni_md.h
    HINTS ENV JAVA_HOME
    PATH_SUFFIXES include/linux include/darwin include/win32 include)
 
if(JNI_INCLUDE_DIR AND JNI_MD_INCLUDE_DIR)
    message(STATUS "JNI headers found: ${JNI_INCLUDE_DIR}")
    message(STATUS "Building subgraph_matching shared library")
 
    add_library(subgraph_matching SHARED src/subgraph_jni.cpp)
    target_link_libraries(subgraph_matching PRIVATE subgraph_compile_options)
    target_include_directories(subgraph_matching PRIVATE
        ${JNI_INCLUDE_DIR}
        ${JNI_MD_INCLUDE_DIR}
    )
else()
    message(WARNING
        "jni.h not found — subgraph_matching shared library will NOT be built.\n"
        "Install a JDK (e.g. openjdk-21-jdk) and re-run cmake to enable it.\n"
        "Tip: set JAVA_HOME to your JDK root if cmake cannot find it automatically.")
endif()