set(CMAKE_CXX_STANDARD 17)

find_package(Boost REQUIRED COMPONENTS system)

add_executable(master
    masterv2.cpp
    headers/task_manager.cpp
    headers/tcp_connection.cpp
)

target_link_libraries(master Boost::system pthread)