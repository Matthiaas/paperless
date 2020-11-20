cmake_minimum_required(VERSION 3.10)
project(paperless)

# Copypaste from: https://stackoverflow.com/questions/23163075/how-to-compile-an-mpi-included-c-program-using-cmake
find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "-lmtbl -rdynamic") # sudo apt install libmtbl-dev
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall ")


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fno-omit-frame-pointer")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=leak")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")

include_directories(third_party)

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
        src/RBTreeMemoryTable.h
        src/BloomFilter.cpp
        src/BloomFilter.h)

set(TEST_SOURCES
        src/tests/tests_main.cpp
        src/tests/ElementTests.cpp
        src/tests/ListQueueTests.cpp
        src/tests/MemoryTableManagerTests.cpp
        src/tests/StatusOrTests.cpp
        src/tests/RBTreeMemoryTableTests.cpp
        src/tests/BloomFilterTests.cpp)

add_subdirectory(third_party/smhasher)

set(MPI_TEST_SOURCES
        src/mpitests/mpi_tests_main.cpp
        src/mpitests/PaperlessKVTest.cpp src/mpitests/PaperlessKVRandomTest.cpp src/LRUCache.cpp src/LRUCache.h)

add_library(paperless STATIC ${LIB_SOURCES})
target_link_libraries(paperless PRIVATE ${MPI_CXX_LIBRARIES})
target_link_libraries(paperless PRIVATE Threads::Threads)
target_link_libraries(paperless PRIVATE smhasher)
target_link_libraries(paperless PRIVATE stdc++fs)

add_executable(main main.cpp)
add_executable(tests
        ${TEST_SOURCES}
        ${LIB_SOURCES}
        )

add_executable(mpi_tests
        ${MPI_TEST_SOURCES}
        ${LIB_SOURCES}
        )

target_link_libraries(main PUBLIC paperless mtbl)

target_link_libraries(tests mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_link_libraries(mpi_tests mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
