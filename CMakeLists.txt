cmake_minimum_required(VERSION 2.8)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3 -DERPC_INFINIBAND=true")

project(ddbs-course-project)

# Include and build eRPC
set(ERPC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/eRPC)
include_directories(${ERPC_DIR}/src ${ERPC_DIR}/third_party/asio/include)
link_directories(${ERPC_DIR}/build)

# Add eRPC build commands
add_custom_target(
    build_erpc
    COMMAND cmake . -DTRANSPORT=infiniband -DPERF=ON -DROCE=OFF
    COMMAND make -j
    WORKING_DIRECTORY ${ERPC_DIR}
)

# Include and build sql-parser
set(SQL_PARSER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/sql-parser)
include_directories(${SQL_PARSER_DIR}/src)
link_directories(${SQL_PARSER_DIR})

add_custom_target(
    build_sql_parser
    COMMAND make -j
    WORKING_DIRECTORY ${SQL_PARSER_DIR}
)

# Ensure dependencies
add_dependencies(build_erpc build_sql_parser)

# Add executables and link with libraries
# rpc_client
add_executable(rpc_client csrc/rpc_client.cc)
add_dependencies(rpc_client build_erpc build_sql_parser)
target_link_libraries(rpc_client erpc pthread numa dl ibverbs mysqlcppconn sqlparser)

# rpc_server
add_executable(rpc_server csrc/rpc_server.cc)
add_dependencies(rpc_server build_erpc build_sql_parser)
target_link_libraries(rpc_server erpc pthread numa dl ibverbs mysqlcppconn sqlparser)

# connector_example
add_executable(connector_example csrc/connector_example.cc)
add_dependencies(connector_example build_sql_parser)
target_link_libraries(connector_example mysqlcppconn)


# client
add_executable(client csrc/client.cc)


# server 
add_executable(server csrc/server.cc)
add_dependencies(server build_erpc build_sql_parser)
target_link_libraries(server erpc pthread numa dl ibverbs mysqlcppconn sqlparser)
