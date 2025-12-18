cmake_minimum_required(VERSION 3.20)
project(hack-one LANGUAGES CXX)
set(FETCHCONTENT_BASE_DIR ${PROJECT_SOURCE_DIR}/deps)
set(PROJECT_3RDS_DIR)

if (DEFINED PROJECT_3RDS_DIR)
    message("DEFINE PROJECT_3RDS_DIR ${PROJECT_3RDS_DIR}")
else()
    set(PROJECT_3RDS_DIR ${PROJECT_SOURCE_DIR}/3rds)
    #message("NEE DEFINE  PROJECT_3RDS_DIR ${PROJECT_3RDS_DIR}")
endif()

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()


# find_package(rapidjson CONFIG QUIET)

# if(NOT rapidjson_FOUND)
#     include(FetchContent)

#     message(STATUS "rapidjson not found, downloading via FetchContent...")

#     set(FETCHCONTENT_SOURCE_DIR_RAPIDJSON ${PROJECT_3RDS_DIR}/rapidjson-1.1.0)
    
#     FetchContent_Declare(rapidjson
#     URL https://github.com/Tencent/rapidjson/archive/refs/tags/v1.1.0.tar.gz)
#     FetchContent_MakeAvailable(rapidjson)
# endif()

# find_package(folly CONFIG QUIET)

# if(NOT folly_FOUND)
#     include(FetchContent)

#     message(STATUS "folly not found, downloading via FetchContent...")

#     FetchContent_Declare(folly
#     URL https://github.com/facebook/folly/releases/download/v2025.11.24.00/folly-v2025.11.24.00.tar.gz)
#     FetchContent_MakeAvailable(folly)
# endif()


find_package(roaring CONFIG QUIET)

if(NOT roaring_FOUND)
    include(FetchContent)

    message(STATUS "roaring not found, downloading via FetchContent...")

    set(FETCHCONTENT_SOURCE_DIR_CROARING ${PROJECT_3RDS_DIR}/CRoaring)

    FetchContent_Declare(roaring
    URL https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v4.5.0.tar.gz)
    FetchContent_MakeAvailable(roaring)
endif()

add_subdirectory(src)

add_subdirectory(tests)

