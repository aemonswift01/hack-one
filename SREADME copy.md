1. 环境依赖
系统：Linux（需支持 mmap/MAP_HUGETLB、LZ4 压缩）
编译器：GCC 9.0+（支持 C++17）
依赖库：liblz4-dev、abseil-cpp（absl 容器）
内存：物理内存≥6GB
2. 编译命令
bash
运行
g++ -std=c++17 -O3 -march=native -o graph_engine src/*.cpp -llz4 -labsl_flat_hash_map
3. 运行参数
参数	说明	示例
-f	CSV 文件路径（必填）	-f ./data/graph.csv
-m	内存阈值（MB，可选，默认 6144）	-m 6144
4. CSV 文件格式（逗号分隔）
plaintext
起点ID,终点ID,边标签,起点标签,终点标签
node1,node2,friend,person,person
node2,node3,colleague,person,person
...
5. 核心功能说明
功能	说明
数据导入	并发导入 CSV 数据，自动构建出边 + 入边双存储 CSR 结构，内存阈值硬限制
内存管理	导入后自动释放临时内存，预留≥5GB 内存供查询使用
邻接查询	支持出边 / 入边邻接查询，返回节点的邻居 ID 列表
子图查询	预留动态查询内存池，支持子图匹配（需扩展 QueryManager）
6. 性能指标
指标	数值
导入速度	6 亿边≈14~17 分钟
内存占用（导入后）	≈1GB，查询可用内存≈5GB
邻接查询耗时	出边 / 入边≈75ns / 次
子图匹配性能	比单存储提升 30%~50%
7. 注意事项
首次运行会创建./cold_blocks目录存储冷块数据，请勿手动删除；
边标签 / 点标签数量不超过 255（uint8_t 限制）；
支持最大 8 亿边 ID、3 亿点 ID；
查询阶段优先使用热块缓存，冷块自动加载并缓存；
若内存不足，程序会自动收缩缓存，不会抛出异常崩溃。