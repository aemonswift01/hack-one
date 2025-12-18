cmake_minimum_required(VERSION 3.12)
project(GraphEngine)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG")

file(GLOB_RECURSE SOURCES "src/*.cpp" "src/*.c")

add_executable(importer src/import_main.cpp src/GraphStorage.cpp src/IdMapper.cpp src/MmappedCSR.cpp)
target_include_directories(importer PRIVATE include src)

add_executable(query_example src/query_example.cpp src/GraphStorage.cpp src/IdMapper.cpp src/MmappedCSR.cpp)
target_include_directories(query_example PRIVATE include src)


target_include_directories(importer PRIVATE include)
target_include_directories(query_example PRIVATE include)
# 新增 main 可执行文件
add_executable(graph_engine src/main.cpp src/GraphStorage.cpp src/IdMapper.cpp src/MmappedCSR.cpp)
target_include_directories(graph_engine PRIVATE include)




# cmake_minimum_required(VERSION 3.20)
# project(hack-one LANGUAGES CXX)
# set(FETCHCONTENT_BASE_DIR ${PROJECT_SOURCE_DIR}/deps)
# set(PROJECT_3RDS_DIR)

# if (DEFINED PROJECT_3RDS_DIR)
#     message("DEFINE PROJECT_3RDS_DIR ${PROJECT_3RDS_DIR}")
# else()
#     set(PROJECT_3RDS_DIR ${PROJECT_SOURCE_DIR}/3rds)
#     message("NEE DEFINE  PROJECT_3RDS_DIR ${PROJECT_3RDS_DIR}")
# endif()

# if(POLICY CMP0135)
#   cmake_policy(SET CMP0135 NEW)
# endif()


# find_package(jsoncpp CONFIG QUIET)

# if(NOT jsoncpp_FOUND)
#     include(FetchContent)

#     message(STATUS "jsoncpp not found, downloading via FetchContent...")

#     set(FETCHCONTENT_SOURCE_DIR_JSONCPP ${PROJECT_3RDS_DIR}/jsoncpp-1.9.4)
    
#     FetchContent_Declare(jsoncpp
#     URL https://github.com/open-source-parsers/jsoncpp/archive/1.9.4.tar.gz)
#     FetchContent_MakeAvailable(jsoncpp)
# endif()

# find_package(folly CONFIG QUIET)

# if(NOT folly_FOUND)
#     include(FetchContent)

#     message(STATUS "folly not found, downloading via FetchContent...")

#     FetchContent_Declare(folly
#     URL https://github.com/facebook/folly/releases/download/v2025.11.24.00/folly-v2025.11.24.00.tar.gz)
#     FetchContent_MakeAvailable(folly)
# endif()


# find_package(roaring CONFIG QUIET)

# if(NOT roaring_FOUND)
#     include(FetchContent)

#     message(STATUS "roaring not found, downloading via FetchContent...")

#     FetchContent_Declare(roaring
#     URL https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v4.5.0.tar.gz)
#     FetchContent_MakeAvailable(roaring)
# endif()
