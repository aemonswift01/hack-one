# 在 src/CMakeLists.txt 中添加
add_subdirectory(storage)

# 创建 src/storage/CMakeLists.txt
add_library(storage
    graph_storage.cc
)

target_include_directories(storage PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(storage
    PRIVATE
    utils
)

add_subdirectory(compute)
add_subdirectory(http)