set(CMAKE_CXX_STANDARD 17)

find_package(Boost REQUIRED COMPONENTS system)

add_executable(master
    master.cpp
    headers/task_manager.cpp
    headers/tcp_connection.cpp
    headers/tcp_server.cpp
)

target_link_libraries(master Boost::system pthread)