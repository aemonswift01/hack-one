cmake_minimum_required(VERSION 3.20)
project(hack-one LANGUAGES CXX)
set(FETCHCONTENT_BASE_DIR ${PROJECT_SOURCE_DIR}/deps)
set(PROJECT_3RDS_DIR)

if (DEFINED PROJECT_3RDS_DIR)
    message("DEFINE PROJECT_3RDS_DIR ${PROJECT_3RDS_DIR}")
else()
    set(PROJECT_3RDS_DIR ${PROJECT_SOURCE_DIR}/3rds)
    message("NEE DEFINE  PROJECT_3RDS_DIR ${PROJECT_3RDS_DIR}")
endif()

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()


find_package(jsoncpp CONFIG QUIET)

if(NOT jsoncpp_FOUND)
    include(FetchContent)

    message(STATUS "jsoncpp not found, downloading via FetchContent...")

    set(FETCHCONTENT_SOURCE_DIR_JSONCPP ${PROJECT_3RDS_DIR}/jsoncpp-1.9.4)
    
    FetchContent_Declare(jsoncpp
    URL https://github.com/open-source-parsers/jsoncpp/archive/1.9.4.tar.gz)
    FetchContent_MakeAvailable(jsoncpp)
endif()

# find_package(folly CONFIG QUIET)

# if(NOT folly_FOUND)
#     include(FetchContent)

#     message(STATUS "folly not found, downloading via FetchContent...")

#     FetchContent_Declare(jsoncpp
#     URL https://github.com/facebook/folly/releases/download/v2025.11.24.00/folly-v2025.11.24.00.tar.gz)
#     FetchContent_MakeAvailable(folly)
# endif()


# find_package(roaring CONFIG QUIET)

# if(NOT roaring_FOUND)
#     include(FetchContent)

#     message(STATUS "roaring not found, downloading via FetchContent...")

#     FetchContent_Declare(jsoncpp
#     URL https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v4.5.0.tar.gz)
#     FetchContent_MakeAvailable(roaring)
# endif()