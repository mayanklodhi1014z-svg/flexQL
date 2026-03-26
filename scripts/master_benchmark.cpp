// ============================================================
//  FlexQL Master Benchmark
//  Runs all performance phases against a live FlexQL server
//  and prints a consolidated results table.
//
//  Usage: ./bin/flexql_master_benchmark [ROW_COUNT] [QUERY_COUNT]
//    ROW_COUNT   – rows to insert for each table phase (default 10,000,000)
//    QUERY_COUNT – point queries to fire in the query phase (default 10,000,000)
// ============================================================

#include <iostream>
#include <iomanip>
#include <chrono>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <algorithm>
#include "flexql.h"

// ── Helpers ─────────────────────────────────────────────────

static int null_cb(void*, int, char**, char**) { return 0; }

using Clock = std::chrono::high_resolution_clock;
using Sec   = std::chrono::duration<double>;

struct Result {
    std::string phase;
    std::string metric;
    double      value;
    std::string unit;
    std::string note;
};

static void check(int rc, const char* ctx) {
    if (rc != FLEXQL_OK) {
        std::cerr << "[FATAL] " << ctx << "\n";
        std::exit(1);
    }
}

static void concurrent_batch_insert(const std::string& table,
                                    const std::string& cols,
                                    const std::vector<std::string>& rows,
                                    int batch_size = 500,
                                    int num_threads = 8) {
    std::vector<std::thread> workers;
    int chunk = std::max(1, (int)rows.size() / num_threads);
    
    for (int t = 0; t < num_threads; t++) {
        int start = t * chunk;
        if (start >= (int)rows.size()) break;
        int end = (t == num_threads - 1) ? (int)rows.size() : start + chunk;

        workers.push_back(std::thread([table, cols, rows, batch_size, start, end]() {
            FlexQL* local_db = nullptr;
            flexql_open("127.0.0.1", 9002, &local_db);
            char* err = nullptr;
            std::string buf;
            buf.reserve(batch_size * 64);
            
            for (int i = start; i < end; i++) {
                if (buf.empty()) {
                    buf = "INSERT INTO " + table + " " + cols + " VALUES ";
                }
                buf += rows[i];
                
                if ((i + 1 - start) % batch_size == 0 || i == end - 1) {
                    flexql_exec(local_db, buf.c_str(), nullptr, nullptr, &err);
                    buf.clear();
                } else {
                    buf += ",";
                }
            }
            flexql_close(local_db);
        }));
    }
    for (auto& w : workers) w.join();
}

// ── Banner ───────────────────────────────────────────────────

static void banner() {
    std::cout << "\n";
    std::cout << "╔══════════════════════════════════════════════════════╗\n";
    std::cout << "║       FlexQL Master Benchmark Suite                 ║\n";
    std::cout << "╚══════════════════════════════════════════════════════╝\n\n";
}

static void section(const std::string& title) {
    std::cout << "\n── " << title << " ";
    for (int i = (int)title.size() + 4; i < 56; i++) std::cout << "─";
    std::cout << "\n";
}

// ── Phase 1: Pure Insert Throughput ─────────────────────────

static Result phase_insert(FlexQL* db, int N) {
    section("Phase 1 · Pure Insert Throughput");
    char* err = nullptr;
    flexql_exec(db, "CREATE TABLE P1 (ID INT PRIMARY KEY, VAL VARCHAR);",
                nullptr, nullptr, &err);

    std::vector<std::string> vals;
    vals.reserve(N);
    for (int i = 0; i < N; i++)
        vals.push_back("(" + std::to_string(i) + ",'v" + std::to_string(i) + "')");

    std::cout << "  Inserting " << N << " rows...\n";
    auto t0 = Clock::now();
    concurrent_batch_insert("P1", "", vals, 500, 8);
    double elapsed = Sec(Clock::now() - t0).count();
    double tput = N / elapsed;

    std::cout << "  ✓ " << std::fixed << std::setprecision(0)
              << tput << " rows/sec  (" << std::setprecision(2)
              << elapsed << "s)\n";
    return {"Insert", "Throughput", tput, "rows/sec",
            std::to_string(N) + " rows, batch=500"};
}

// ── Phase 2: Mixed Workload (writes + concurrent reads) ──────

static Result phase_mixed(FlexQL* db, int N) {
    section("Phase 2 · Mixed Workload  (inserts + concurrent point reads)");
    char* err = nullptr;
    flexql_exec(db, "CREATE TABLE P2 (ID INT PRIMARY KEY);",
                nullptr, nullptr, &err);

    std::atomic<bool> inserting{true};
    std::atomic<int>  read_ops{0};

    // Concurrent reader — point queries cycling through up to 1M IDs
    std::thread reader([&]() {
        FlexQL* rdb = nullptr;
        flexql_open("127.0.0.1", 9002, &rdb);
        int probe = 0;
        while (inserting.load()) {
            std::string q = "SELECT * FROM P2 WHERE ID = "
                          + std::to_string(probe) + ";";
            flexql_exec(rdb, q.c_str(), null_cb, nullptr, nullptr);
            probe = (probe + 997) % std::min(N, 1000000);
            read_ops++;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        flexql_close(rdb);
    });

    std::cout << "  Inserting " << N << " rows with concurrent reader...\n";
    auto t0 = Clock::now();
    std::vector<std::thread> workers;
    int chunk = std::max(1, N / 8);
    for (int t = 0; t < 8; t++) {
        int start = t * chunk;
        if (start >= N) break;
        int end = (t == 7) ? N : start + chunk;

        workers.push_back(std::thread([start, end]() {
            FlexQL* ldb = nullptr;
            flexql_open("127.0.0.1", 9002, &ldb);
            char* err = nullptr;
            std::string row_batch;
            row_batch.reserve(500 * 32);
            for (int i = start; i < end; i++) {
                row_batch += "INSERT INTO P2 VALUES (" + std::to_string(i) + ");\n";
                if ((i + 1 - start) % 400 == 0 || i == end - 1) {
                    flexql_exec(ldb, row_batch.c_str(), nullptr, nullptr, &err);
                    row_batch.clear();
                }
            }
            flexql_close(ldb);
        }));
    }
    for (auto& w : workers) w.join();
    inserting.store(false);
    reader.join();
    double elapsed = Sec(Clock::now() - t0).count();
    double tput = N / elapsed;

    std::cout << "  ✓ " << std::fixed << std::setprecision(0)
              << tput << " inserts/sec  (" << std::setprecision(2)
              << elapsed << "s)  concurrent reads: " << read_ops.load() << "\n";
    return {"Mixed", "Insert throughput under reads", tput, "rows/sec",
            std::to_string(read_ops.load()) + " concurrent reads survived"};
}

// ── Phase 3: Point Query Throughput (index lookups) ──────────

static Result phase_query(FlexQL* db, int N, int Q) {
    section("Phase 3 · Point Query Throughput  (PK index lookups)");
    char* err = nullptr;
    flexql_exec(db, "CREATE TABLE P3 (ID INT PRIMARY KEY, VAL VARCHAR);",
                nullptr, nullptr, &err);

    // Pre-fill
    std::cout << "  Pre-filling " << N << " rows...\n";
    std::vector<std::string> vals;
    vals.reserve(N);
    for (int i = 0; i < N; i++)
        vals.push_back("(" + std::to_string(i) + ",'Val" + std::to_string(i) + "')");
    concurrent_batch_insert("P3", "", vals, 500, 8);

    // Query phase
    std::cout << "  Firing " << Q << " batched point queries...\n";
    std::vector<std::thread> qw;
    int qchunk = std::max(1, Q / 8);
    auto t0 = Clock::now();
    for (int t = 0; t < 8; t++) {
        int start = t * qchunk;
        if (start >= Q) break;
        int end = (t == 7) ? Q : start + qchunk;

        qw.push_back(std::thread([start, end, N]() {
            FlexQL* ldb = nullptr;
            flexql_open("127.0.0.1", 9002, &ldb);
            char* err = nullptr;
            std::string batch;
            batch.reserve(200 * 50);
            for (int i = start; i < end; i++) {
                int id = i % N;
                batch += "SELECT * FROM P3 WHERE ID = " + std::to_string(id) + ";\n";
                if ((i + 1 - start) % 200 == 0 || i == end - 1) {
                   flexql_exec(ldb, batch.c_str(), nullptr, nullptr, &err);
                    batch.clear();
                }
            }
            flexql_close(ldb);
        }));
    }
    for (auto& w : qw) w.join();
    double elapsed = Sec(Clock::now() - t0).count();
    double tput    = Q / elapsed;
    double lat_us  = elapsed * 1e6 / Q;

    std::cout << "  ✓ " << std::fixed << std::setprecision(0)
              << tput << " queries/sec  lat=" << std::setprecision(2)
              << lat_us << "µs  (" << elapsed << "s)\n";
    return {"Point Query", "Throughput", tput, "queries/sec",
            std::to_string(Q) + " queries, " + std::to_string(N) + "-row table, lat="
            + std::to_string((int)lat_us) + "µs"};
}

// ── Phase 4: INNER JOIN  (hash join) ─────────────────────────

static Result phase_join(FlexQL* db, int emp_count, int dept_count, int iters) {
    section("Phase 4 · INNER JOIN Performance  (hash join)");
    char* err = nullptr;
    flexql_exec(db, "CREATE TABLE DEPTS2 (ID INT PRIMARY KEY, DNAME VARCHAR);",
                nullptr, nullptr, &err);
    flexql_exec(db, "CREATE TABLE EMPS2  (ID INT PRIMARY KEY, DEPTID INT, ENAME VARCHAR);",
                nullptr, nullptr, &err);

    // Fill depts
    std::vector<std::string> dv;
    dv.reserve(dept_count);
    for (int i = 0; i < dept_count; i++)
        dv.push_back("(" + std::to_string(i) + ",'Dept" + std::to_string(i) + "')");
    std::cout << "  Pre-filling " << dept_count << " departments...\n";
    concurrent_batch_insert("DEPTS2", "", dv, 500, 4);

    // Fill emps
    std::vector<std::string> ev;
    ev.reserve(emp_count);
    for (int i = 0; i < emp_count; i++)
        ev.push_back("(" + std::to_string(i) + ","
                     + std::to_string(i % dept_count)
                     + ",'Emp"  + std::to_string(i) + "')");
    std::cout << "  Pre-filling " << emp_count << " employees...\n";
    concurrent_batch_insert("EMPS2", "", ev, 500, 8);

    std::string jq = "SELECT EMPS2.ENAME, DEPTS2.DNAME "
                     "FROM EMPS2 INNER JOIN DEPTS2 ON EMPS2.DEPTID = DEPTS2.ID;";

    std::cout << "  Executing " << iters << " JOIN queries...\n";
    auto t0 = Clock::now();
    for (int i = 0; i < iters; i++)
        flexql_exec(db, jq.c_str(), null_cb, nullptr, &err);
    double elapsed = Sec(Clock::now() - t0).count();
    double tput    = iters / elapsed;
    double lat_ms  = elapsed * 1000.0 / iters;

    std::cout << "  ✓ " << std::fixed << std::setprecision(3)
              << tput << " JOINs/sec  lat=" << std::setprecision(1)
              << lat_ms << "ms  (" << std::setprecision(2) << elapsed << "s)\n";
    return {"JOIN", "Throughput", tput, "JOINs/sec",
            std::to_string(emp_count) + " × " + std::to_string(dept_count)
            + ", lat=" + std::to_string((int)lat_ms) + "ms"};
}

// ── Final Summary Table ──────────────────────────────────────

static void print_summary(const std::vector<Result>& results) {
    std::cout << "\n\n";
    std::cout << "╔══════════════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║                   FlexQL Master Benchmark — Results                     ║\n";
    std::cout << "╠══════════╦══════════════════════════════╦══════════════╦════════════════╣\n";
    std::cout << "║ Phase    ║ Metric                       ║ Value        ║ Notes          ║\n";
    std::cout << "╠══════════╬══════════════════════════════╬══════════════╬════════════════╣\n";
    for (const auto& r : results) {
        std::ostringstream val;
        val << std::fixed << std::setprecision(0) << r.value << " " << r.unit;
        std::cout << "║ " << std::left << std::setw(9)  << r.phase
                  << "║ "            << std::setw(29) << r.metric
                  << "║ "            << std::setw(13) << val.str()
                  << "║ "            << std::setw(15) << r.note.substr(0, 15)
                  << "║\n";
    }
    std::cout << "╚══════════╩══════════════════════════════╩══════════════╩════════════════╝\n\n";
}

// ── Main ─────────────────────────────────────────────────────

int main(int argc, char** argv) {
    int ROW_COUNT   = 10'000'000;
    int QUERY_COUNT = 10'000'000;
    if (argc > 1) ROW_COUNT   = std::stoi(argv[1]);
    if (argc > 2) QUERY_COUNT = std::stoi(argv[2]);

    banner();
    std::cout << "  Config: ROW_COUNT=" << ROW_COUNT
              << "  QUERY_COUNT=" << QUERY_COUNT << "\n";
    std::cout << "  Server : 127.0.0.1:9000\n";

    // Single persistent connection for non-threaded phases
    FlexQL* db = nullptr;
    check(flexql_open("127.0.0.1", 9002, &db), "connect to FlexQL server");
    std::cout << "  ✓ Connected\n";

    std::vector<Result> results;

    auto t_total = Clock::now();

    results.push_back(phase_insert(db, ROW_COUNT));
    results.push_back(phase_mixed (db, ROW_COUNT));
    results.push_back(phase_query (db, ROW_COUNT, QUERY_COUNT));

    // JOIN: 100K employees × 100 depts, 10 iterations (scales with ROW_COUNT)
    int emp   = std::min(ROW_COUNT, 100'000);
    int dept  = 100;
    int iters = 10;
    results.push_back(phase_join(db, emp, dept, iters));

    double total_elapsed = Sec(Clock::now() - t_total).count();

    flexql_close(db);

    print_summary(results);
    std::cout << "  Total wall time: " << std::fixed << std::setprecision(1)
              << total_elapsed << "s\n\n";
    return 0;
}
