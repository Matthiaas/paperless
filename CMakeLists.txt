cmake_minimum_required(VERSION 3.10)
project(paperless)

option(IS_CLUSTER "Indicates whether cmake is run on the cluster or not." OFF)
option(VECTORIZE "Use SIMD for comparison." OFF)

# Copypaste from: https://stackoverflow.com/questions/23163075/how-to-compile-an-mpi-included-c-program-using-cmake
find_package(MPI REQUIRED)
MESSAGE (STATUS "MPI_CXX_COMPILER ${MPI_CXX_COMPILER}")

if (NOT "${MPI_CXX_COMPILER}" STREQUAL "MPI_CXX_COMPILER-NOTFOUND")
    set(CMAKE_CXX_COMPILER "${MPI_CXX_COMPILER}")
endif()

include_directories(SYSTEM ${MPI_INCLUDE_PATH})

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

#
# DEFAULT FLAGS
#
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "-Wall")

if(VECTORIZE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2 -DVECTORIZE")
  message("-- Set Option 'VECTORIZE': Using SIMD for comparison.")
endif()

#
# RELEASE FLAGS
#
# sudo apt install libmtbl-dev
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}  -O3")

#
# DEBUG FLAGS
#
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -rdynamic")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer")
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=leak")
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread")


if (IS_CLUSTER)
  # Non-global LD_LIBRARY_PATH equivalent.
  set(CMAKE_INSTALL_RPATH "~/usr/lib")
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
  link_directories(~/usr/lib)
  include_directories(~/usr/include)
endif()

link_directories(papyrus/install/lib)
include_directories(third_party)

set(LIB_SOURCES
        src/PaperlessKV.cpp
        src/PaperlessKV.h
        src/Responder.cpp
        src/Responder.h
        src/RemoteOperator.cpp
        src/RemoteOperator.h
        src/Message.h
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
        src/BloomFilter.h
        src/Common.h)

set(TEST_SOURCES
        src/tests/tests_main.cpp
        src/tests/ElementTests.cpp
        src/tests/ListQueueTests.cpp
        src/tests/MemoryTableManagerTests.cpp
        src/tests/StatusOrTests.cpp
        src/tests/RBTreeMemoryTableTests.cpp
        src/tests/LRUTreeTests.cpp
        src/tests/LRUHashTests.cpp
        src/tests/BloomFilterTests.cpp src/tests/StorageManagerTests.cpp)

add_subdirectory(third_party/smhasher)

set(MPI_TEST_SOURCES
        src/mpitests/mpi_tests_main.cpp
        src/mpitests/PaperlessKVTest.cpp
        src/mpitests/PaperlessKVStressTest.cpp
        src/mpitests/PaperLessKVFriend.h)

set(BENCHMARK_SOURCES
        src/benchmark/OptionReader.cpp
        src/benchmark/OptionReader.h
        )

set(THE_GREAT_BECHMARK_SOURCES
        src/benchmark/KVWrapper.h
        src/benchmark/theGreatBenchmark.cpp)

set(THROUGHPUT_BECHMARK_SOURCES
        src/benchmark/KVWrapper.h
        src/benchmark/throughput.cpp)


set(WOKRLOAD_BECHMARK_SOURCES
        src/benchmark/workload.cpp)

set(BASIC_BECHMARK_SOURCES
        src/benchmark/basic.cpp src/FutureQueryInfo.h)

set(MTM_COMPARISON_SOURCES
        src/benchmark/mtm_comparison.cpp)

set(MPI_PROBE_MULTIPLE_THREADS_SOURCES
        src/benchmark/mpi_probe_multiple_threads.cpp)

set(CMP_VECTOR_BENCHMARK_SOURCES
        src/benchmark/cmp_vector_benchmark.cpp)

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

add_executable(mpi_probe_multiple_threads
        ${MPI_PROBE_MULTIPLE_THREADS_SOURCES})

add_executable(thegreatbenchmark_paperless
        ${BENCHMARK_SOURCES}
        ${THE_GREAT_BECHMARK_SOURCES}
        ${LIB_SOURCES})

add_executable(thegreatbenchmark_paperless_hash_cache
        ${BENCHMARK_SOURCES}
        ${THE_GREAT_BECHMARK_SOURCES}
        ${LIB_SOURCES})

add_executable(thegreatbenchmark_papyrus
        ${BENCHMARK_SOURCES}
        ${THE_GREAT_BECHMARK_SOURCES}
        ${LIB_SOURCES})

add_executable(throughput_Ipaperless
        ${BENCHMARK_SOURCES}
        ${THROUGHPUT_BECHMARK_SOURCES}
        ${LIB_SOURCES})

add_executable(throughput_paperless
        ${BENCHMARK_SOURCES}
        ${THROUGHPUT_BECHMARK_SOURCES}
        ${LIB_SOURCES})

add_executable(throughput_papyrus
        ${BENCHMARK_SOURCES}
        ${THROUGHPUT_BECHMARK_SOURCES}
        ${LIB_SOURCES})

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

add_executable(cmp_vector
        ${CMP_VECTOR_BENCHMARK_SOURCES}
        ${LIB_SOURCES})

target_link_libraries(main PUBLIC paperless mtbl)




target_link_libraries(thegreatbenchmark_paperless mtbl papyruskv smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_compile_definitions(thegreatbenchmark_paperless PRIVATE PAPERLESS_BENCHMARK)
target_link_libraries(thegreatbenchmark_paperless_hash_cache mtbl papyruskv smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_compile_definitions(thegreatbenchmark_paperless_hash_cache PRIVATE PAPERLESS_BENCHMARK PAPERLESS_USE_HASH_CACHE)
target_link_libraries(thegreatbenchmark_papyrus mtbl papyruskv smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_compile_definitions(thegreatbenchmark_papyrus PRIVATE PAPYRUS_BENCHMARK)

target_link_libraries(throughput_Ipaperless mtbl papyruskv smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_compile_definitions(throughput_Ipaperless PRIVATE I_PAPERLESS_BENCHMARK)
target_link_libraries(throughput_paperless mtbl papyruskv smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_compile_definitions(throughput_paperless PRIVATE PAPERLESS_BENCHMARK)
target_link_libraries(throughput_papyrus mtbl papyruskv smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_compile_definitions(throughput_papyrus PRIVATE PAPYRUS_BENCHMARK)

target_link_libraries(cmp_vector mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_link_libraries(basic mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_link_libraries(workload mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_link_libraries(mtm_comparison mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_link_libraries(tests mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_link_libraries(mpi_tests mtbl smhasher stdc++fs ${MPI_CXX_LIBRARIES} Threads::Threads)
target_link_libraries(mpi_probe_multiple_threads ${MPI_CXX_LIBRARIES} pthread)
