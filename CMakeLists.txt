cmake_minimum_required(VERSION 3.15)
project(paperless)

set(CMAKE_CXX_STANDARD 17)

add_executable(paperless main.cpp src/PaperlessKV.cpp src/PaperlessKV.h src/MemoryTable.cpp src/MemoryTable.h src/StorageManager.cpp src/StorageManager.h src/Element.h src/CircularNonBlockingQueue.cpp src/CircularNonBlockingQueue.h src/MemoryTableManager.cpp src/MemoryTableManager.h src/Types.h src/StatusOr.cpp src/StatusOr.h src/Status.h)