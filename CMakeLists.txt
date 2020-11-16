cmake_minimum_required(VERSION 3.10)
project(paperless)

# Copypaste from: https://stackoverflow.com/questions/23163075/how-to-compile-an-mpi-included-c-program-using-cmake
find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(CMAKE_CXX_FLAGS "-lmtbl -Wall") # sudo apt install libmtbl-dev

set(CMAKE_CXX_STANDARD 17)



include_directories(third_party)
#include_directories(src)


set(LIB_SOURCES
        src/PaperlessKV.cpp
        src/PaperlessKV.h
        src/MemoryTable.h
        src/StorageManager.cpp
        src/StorageManager.h
        src/Element.h
        src/Element.cpp
        src/MemoryTableManager.h
        src/Types.h
        src/StatusOr.h
        src/Status.h
        src/RBTreeMemoryTable.cpp
        src/RBTreeMemoryTable.h )

set(TEST_SOURCES
        src/tests/tests_main.cpp
        src/tests/ElementTests.cpp
        src/tests/ListQueueTests.cpp
        src/tests/MemoryTableManagerTests.cpp
        src/tests/StatusOrTests.cpp
        src/tests/RBTreeMemoryTableTests.cpp)

add_library(paperless STATIC ${LIB_SOURCES})
target_link_libraries(paperless PRIVATE ${MPI_CXX_LIBRARIES})
target_link_libraries(paperless PRIVATE Threads::Threads)

add_executable(main main.cpp)
add_executable(tests
        ${TEST_SOURCES}
        ${LIB_SOURCES}
        )

target_link_libraries(main PUBLIC paperless)

target_link_libraries(tests mtbl ${MPI_CXX_LIBRARIES} Threads::Threads)
