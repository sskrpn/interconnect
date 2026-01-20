cmake_minimum_required(VERSION 4.2)

set(CMAKE_CXX_STANDARD 17)

find_package(Boost REQUIRED COMPONENTS system)

file(GLOB SOURCES "master.cpp" "slavev2.cpp")

foreach(SOURCE_PATH ${SOURCES})
if(SOURCE_PATH MATCHES "master.cpp")
    get_filename_component(EXECUTABLE_NAME ${SOURCE_PATH} NAME_WE)
    add_executable(master
        ${SOURCE_PATH}
        headers/task_manager.cpp
        headers/tcp_connection_master.cpp
        headers/tcp_server.cpp
    )
    target_link_libraries(${EXECUTABLE_NAME} Boost::system pthread)
elseif(SOURCE_PATH MATCHES "slavev2.cpp")
    get_filename_component(slave ${SOURCE_PATH} NAME_WE)
    add_executable(slave
        ${SOURCE_PATH}
        headers/tcp_connection_slave.cpp
    )
    target_link_libraries(slave Boost::system pthread)
endif()
endforeach(SOURCE_PATH ${SOURCES})