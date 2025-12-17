#include "s_other.h"
#include "s_storage.h"
#include "s_cache.h"
#include "load_csv.h"
#include "s_query.h"
#include <iostream>
#include <chrono>
#include <sys/resource.h>

using namespace std;
using namespace chrono;

uint64_t CountCSVLines(const string& path) {
    ifstream fin(path);
    if (!fin) throw runtime_error("Open CSV failed: " + path);
    uint64_t cnt = 0;
    string line;
    while (getline(fin, line)) cnt++;
    return cnt - 1;  // 减表头
}

int main(int argc, char* argv[]) {
    string csv_path;
    uint64_t mem_limit_mb = 6144;

    for (int opt; (opt = getopt(argc, argv, "f:m:")) != -1;) {
        switch (opt) {
            case 'f': csv_path = optarg; break;
            case 'm': mem_limit_mb = stoull(optarg); break;
            default:
                cerr << "Usage: " << argv[0] << " -f <csv_path> [-m <mem_limit_mb>]" << endl;
                return 1;
        }
    }

    if (csv_path.empty() || !fs::exists(csv_path)) {
        cerr << "Error: CSV path is required and exists" << endl;
        return 1;
    }

    SetProcessMemLimit(mem_limit_mb);

    try {
        MemoryPoolManager mem_pool;
        BlockedCSR csr("./cold_blocks");
        CacheManager cache(&csr, &mem_pool);
        CSVLoader loader;

        StringIdMapping point_id_map;
        LabelMapping point_label_map;
        LabelMapping edge_label_map;

        cout << "CSV lines: " << CountCSVLines(csv_path) << endl;
        auto start = high_resolution_clock::now();

        loader.LoadSingleCSVConcurrent(csv_path, point_id_map, point_label_map, edge_label_map, csr, cache, mem_limit_mb);

        cache.ReleaseImportTempMem();
        auto end = high_resolution_clock::now();
        double dur = duration<double>(end - start).count();

        uint64_t post_mem = GetUsedMemory() / 1024 / 1024;
        cout << "Import done! Time: " << dur << "s, Mem used: " << post_mem << "MB, Free for query: " << (mem_limit_mb - post_mem) << "MB" << endl;

        QueryManager query(&csr, &cache, &mem_pool);
        query.AllocQueryMem(mem_limit_mb - post_mem - 1000);  // 预留1G系统内存

        // 示例查询
        uint32_t test_point = 100;
        auto neighbors_out = query.GetNeighbors(test_point, true);
        auto neighbors_in = query.GetNeighbors(test_point, false);
        cout << "Point " << test_point << " out neighbors: " << neighbors_out.size() << endl;
        cout << "Point " << test_point << " in neighbors: " << neighbors_in.size() << endl;

    } catch (const bad_alloc& e) {
        cerr << "Memory exceed limit (safe exit)" << endl;
        return 1;
    } catch (const exception& e) {
        cerr << "Error: " << e.what() << endl;
        return 1;
    }

    return 0;
}