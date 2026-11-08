cmake_minimum_required(VERSION 3.5...3.29)

set(TARGET_NAME spark_dialect)
project(${TARGET_NAME})

include_directories(src/include)

set(EXTENSION_SOURCES
    src/spark_dialect_extension.cpp
    src/spark_tokenizer.cpp
    src/spark_grammar.cpp
    src/spark_functions.cpp
    src/spark_macros.cpp
    src/spark_library.cpp
    src/spark_datetime.cpp
    src/spark_formats.cpp
    src/spark_extras.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
