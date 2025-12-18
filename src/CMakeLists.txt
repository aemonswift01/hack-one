


file(GLOB SOURCES CONFIGURE_DEPENDS *.cpp)

add_executable(main ${SOURCES} )

# target_link_libraries(main
#     PRIVATE
#     xxHash
# )