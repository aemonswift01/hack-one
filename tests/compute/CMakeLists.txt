
file(GLOB SOURCES CONFIGURE_DEPENDS *.cpp)

add_executable(compute_test ${SOURCES})

target_link_libraries(compute_test PRIVATE compute)