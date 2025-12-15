file(GLOB SOURCES CONFIGURE_DEPENDS *.cc)

add_library(compute ${SOURCES})


target_include_directories(compute
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)
