
cmake_minimum_required(VERSION 3.16)

project(MonacGraphPCST LANGUAGES CXX)

# 使用 C++17，与 MonacGraph 现有 C++ 模块保持一致
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# 官方 PCSTFast 源码目录
set(PCST_FAST_DIR
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/pcst_fast/src
)

# 检查官方源码
if(NOT EXISTS "${PCST_FAST_DIR}/pcst_fast.cc")
    message(FATAL_ERROR "pcst_fast.cc not found!")
endif()

# 查找 JNI 头文件
find_path(JNI_INCLUDE_DIR
    jni.h
    HINTS "$ENV{JAVA_HOME}"
    PATH_SUFFIXES include
    REQUIRED
)

find_path(JNI_PLATFORM_INCLUDE_DIR
    jni_md.h
    HINTS "$ENV{JAVA_HOME}"
    PATH_SUFFIXES include/linux
    REQUIRED
)

message(STATUS "JNI include: ${JNI_INCLUDE_DIR}")
message(STATUS "JNI platform include: ${JNI_PLATFORM_INCLUDE_DIR}")

# 动态库输出目录
set(PCST_OUTPUT_DIR
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/main/resources/storage/linux
)

file(MAKE_DIRECTORY "${PCST_OUTPUT_DIR}")

# 编译 JNI + PCSTFast
add_library(pcst SHARED
    src/pcst_jni.cpp
    ${PCST_FAST_DIR}/pcst_fast.cc
)

target_include_directories(pcst PRIVATE
    ${JNI_INCLUDE_DIR}
    ${JNI_PLATFORM_INCLUDE_DIR}
    ${PCST_FAST_DIR}
)

target_compile_options(pcst PRIVATE
    -O3
    -Wall
    -Wextra
)

# 生成 libpcst.so
set_target_properties(pcst PROPERTIES
    OUTPUT_NAME pcst
    LIBRARY_OUTPUT_DIRECTORY "${PCST_OUTPUT_DIR}"
)

message(STATUS "PCST JNI module configured successfully.")
