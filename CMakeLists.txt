cmake_minimum_required(VERSION 3.15)
project(paperless)

set(CMAKE_CXX_FLAGS -lmtbl)

set(CMAKE_CXX_STANDARD 17)

add_library(paperless STATIC  src/PaperlessKV.cpp src/PaperlessKV.h src/MemoryTable.cpp
    src/MemoryTable.h src/StorageManager.cpp src/StorageManager.h src/Element.h
    src/CircularNonBlockingQueue.cpp src/CircularNonBlockingQueue.h)

add_executable(main main.cpp )
target_link_libraries(main PUBLIC paperless)