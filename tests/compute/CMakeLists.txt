
file(GLOB SOURCES CONFIGURE_DEPENDS *.cc)

add_executable(compute_test ${SOURCES})

target_link_libraries(compute_test PRIVATE compute)