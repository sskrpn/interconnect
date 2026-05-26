cmake_minimum_required(VERSION 3.20)
project(interconnect LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Boost 1.70 REQUIRED CONFIG)
find_package(Threads REQUIRED)

if(UNIX AND NOT APPLE)
    add_executable(master
        src/main/master.cpp
        src/task_manager.cpp
        src/tcp_connection_master.cpp
    )
    target_include_directories(master PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/headers
    )
    target_link_libraries(master PRIVATE Boost::headers Threads::Threads)
else()
    message(STATUS "Skipping 'master' build: master uses Linux-only syscalls (mmap/fstat/open).")
endif()

add_executable(slave
    src/main/slave.cpp
    src/tcp_connection_slave.cpp
)
target_include_directories(slave PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/headers
)
target_link_libraries(slave PRIVATE Boost::headers Threads::Threads)
