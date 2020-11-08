cmake_minimum_required(VERSION 3.10)
project(paperless)

set(CMAKE_CXX_FLAGS -lmtbl) # sudo apt install libmtbl-dev

set(CMAKE_CXX_STANDARD 17)

include_directories(third_party)
include_directories(/usr/include/mpi/)
#include_directories(src)


set(LIB_SOURCES
        src/PaperlessKV.cpp
        src/PaperlessKV.h
        src/MemoryTable.cpp
        src/MemoryTable.h
        src/StorageManager.cpp
        src/StorageManager.h
        src/Element.h
        src/CircularNonBlockingQueue.cpp
        src/CircularNonBlockingQueue.h
        src/MemoryTableManager.cpp
        src/MemoryTableManager.h
        src/Types.h src/StatusOr.cpp
        src/StatusOr.h
        src/Status.h
        )

set(TEST_SOURCES
        src/tests/tests_main.cpp
        src/tests/ElementTests.cpp
        )

add_library(paperless STATIC ${LIB_SOURCES})

add_executable(main main.cpp)
add_executable(tests
        ${TEST_SOURCES}
        src/Element.h # TODO: Remove this line and uncomment next line when the other
        # stuff starts compiling!
        # ${LIB_SOURCES}
        )

target_link_libraries(main PUBLIC paperless)
target_link_libraries(tests mtbl)
