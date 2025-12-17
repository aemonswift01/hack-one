1. 编译命令
bash
运行
# 进入代码目录
cd graph_storage

# 编译
go build -o graph_engine .

# 运行（-f 指定CSV路径，-m 指定内存限制）
./graph_engine -f ./graph.csv -m 6144
2. 核心特性
模块化拆分：按功能拆分为 8 个文件，职责清晰；
内存监控：独立MemoryMonitor模块实时监控内存占用；
并发安全：基于sync.Map和sync.Mutex保证并发安全；
内存限制：严格控制 6GB 内存占用，导入后自动释放临时内存；
迭代器模式：实现标准迭代器接口，支持邻接查询。
3. 注意事项
需 Linux 系统运行（依赖 mmap、大页内存等系统调用）；
建议以 root 权限运行（大页内存分配需要管理员权限）；
CSV 文件格式需严格遵循：起点ID,终点ID,边标签,起点标签,终点标签；
首次运行会创建./cold_blocks目录存储冷块数据，请勿手动删除。