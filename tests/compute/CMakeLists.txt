cmake
编辑
# 编译测试程序
add_executable(test_compute *.cpp)

# 链接 compute 库（这样就能访问 MyClass）
target_link_libraries(test_compute PRIVATE compute_lib)

# 如果你没建库，也可以直接包含源文件（不推荐用于大型项目）：
# target_sources(test_myclass PRIVATE ../../src/compute/MyClass.cpp)
# target_include_directories(test_myclass PRIVATE ../../src)