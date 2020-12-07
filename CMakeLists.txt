cmake_minimum_required(VERSION 3.10)
project(paperless)

option(IS_CLUSTER "Indicates whether cmake is run on the cluster or not." OFF)

# Copypaste from: https://stackoverflow.com/questions/23163075/how-to-compile-an-mpi-included-c-program-using-cmake
find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 20)

# sudo apt install libmtbl-dev
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -O3")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -g -rdynamic")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer")
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=leak")
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread")

if (IS_CLUSTER)
  # Non-global LD_LIBRARY_PATH equivalent.
  set(CMAKE_INSTALL_RPATH "~/usr/lib")
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
  link_directories(~/usr/lib)
  include_directories(~/usr/include)
endif()

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
        src/LRUTreeCache.cpp
        src/LRUTreeCache.h
        src/LRUHashCache.cpp
        src/LRUHashCache.h
        src/BloomFilter.cpp
        src/BloomFilter.h)

set(TEST_SOURCES
        src/tests/tests_main.cpp
        src/tests/ElementTests.cpp
        src/tests/ListQueueTests.cpp
        src/tests/MemoryTableManagerTests.cpp
        src/tests/StatusOrTests.cpp
        src/tests/RBTreeMemoryTableTests.cpp
        src/tests/LRUTreeTests.cpp
        src/tests/LRUHashTests.cpp
        src/tests/BloomFilterTests.cpp src/tests/TestUtils.h src/tests/TestUtils.cpp)

add_subdirectory(third_party/smhasher)

set(MPI_TEST_SOURCES
        src/mpitests/mpi_tests_main.cpp
        src/mpitests/PaperlessKVTest.cpp
        src/mpitests/PaperlessKVStressTest.cpp
        src/mpitests/PaperLessKVFriend.h)

set(BENCHMARK_SOURCES
        src/benchmark/OptionReader.cpp
        src/benchmark/OptionReader.h)

set(WOKRLOAD_BECHMARK_SOURCES
        src/benchmark/workload.cpp)

set(BASIC_BECHMARK_SOURCES
        src/benchmark/basic.cpp)

set(MTM_COMPARISON_SOURCES
        src/benchmark/mtm_comparison.cpp)

add_library(paperless STATIC ${LIB_SOURCES})
target_link_libraries(paperless PRIVATE ${MPI_CXX_LIBRARIES})
target_link_libraries(paperless PRIVATE Threads::Threads)
target_link_libraries(paperless PRIVATE smhasher)
target_link_libraries(paperless PRIVATE stdc++fs)
target_link_libraries(paperless PRIVATE mtbl)

add_executable(main main.cpp)
add_executable(tests
        ${TEST_SOURCES}
        ${LIB_SOURCES}
        )

add_executable(mpi_tests
        ${BENCHMARK_SOURCES}
        ${MPI_TEST_SOURCES}
        ${LIB_SOURCES}
        )

add_executable(workload
        ${BENCHMARK_SOURCES}
        ${WOKRLOAD_BECHMARK_SOURCES}
        ${LIB_SOURCES})

add_executable(basic
        ${BENCHMARK_SOURCES}
        ${BASIC_BECHMARK_SOURCES}
        ${LIB_SOURCES})

add_executable(mtm_comparison
        ${MTM_COMPARISON_SOURCES}
        ${LIB_SOURCES})

target_link_libraries(main PUBLIC paperless mtbl)

target_link_libraries(basic mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_link_libraries(workload mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_link_libraries(mtm_comparison mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_link_libraries(tests mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_link_libraries(mpi_tests mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
