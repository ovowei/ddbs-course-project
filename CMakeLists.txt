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
# # Rpcclient
# add_executable(Rpcclient Rpcclient.cc)
# add_dependencies(Rpcclient build_erpc build_sql_parser)
# target_link_libraries(Rpcclient erpc pthread numa dl ibverbs mysqlcppconn sqlparser)

# # Rpcserver
# add_executable(Rpcserver Rpcserver.cc)
# add_dependencies(Rpcserver build_erpc build_sql_parser)
# target_link_libraries(Rpcserver erpc pthread numa dl ibverbs mysqlcppconn sqlparser)

# # db-impl
# add_executable(db-impl db-impl.cc)
# add_dependencies(db-impl build_erpc build_sql_parser)
# target_link_libraries(db-impl erpc pthread numa dl ibverbs mysqlcppconn sqlparser)

# # parser_example
# add_executable(parser_example parser_example.cc)
# add_dependencies(parser_example build_sql_parser)
# target_link_libraries(parser_example sqlparser)

# connector_example
add_executable(connector_example csrc/connector_example.cc)
add_dependencies(connector_example build_sql_parser)
target_link_libraries(connector_example mysqlcppconn)

# # control
# add_executable(control control.cc)
# add_dependencies(control build_erpc build_sql_parser)
# target_link_libraries(control erpc pthread numa dl ibverbs mysqlcppconn sqlparser)
