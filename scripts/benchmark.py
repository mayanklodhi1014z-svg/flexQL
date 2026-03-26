import socket
import struct
import time
import sys

def send_query(s, query):
    s.sendall(struct.pack('!I', len(query)))
    s.sendall(query.encode())
    
    resp_len_data = s.recv(4)
    if not resp_len_data: return None
    resp_len = struct.unpack('!I', resp_len_data)[0]
    
    data = b""
    while len(data) < resp_len:
        chunk = s.recv(resp_len - len(data))
        if not chunk: break
        data += chunk
    return data.decode()

def benchmark():
    num_rows = 100000 
    if len(sys.argv) > 1:
        num_rows = int(sys.argv[1])
        
    print(f"Connecting to server...")
    s = socket.socket()
    try:
        s.connect(('127.0.0.1', 9000))
    except Exception as e:
        print("Server not running:", e)
        return

    print(f"--- Creating Benchmark Table ---")
    send_query(s, "CREATE TABLE BENCHMARK(ID INT PRIMARY KEY, VAL VARCHAR);")

    print(f"\n--- Inserting {num_rows} rows (Testing WAL Disk Append Speed) ---")
    start_time = time.time()
    for i in range(num_rows):
        send_query(s, f"INSERT INTO BENCHMARK VALUES({i}, 'Data_{i}');")
        if i % 10000 == 0 and i > 0:
            print(f"Inserted {i} rows so far...")
    end_time = time.time()
    insert_duration = end_time - start_time
    print(f"Finished. Total INSERT time: {insert_duration:.4f} seconds")
    print(f"--> INSERT Throughput: {num_rows / insert_duration:,.2f} operations/sec")

    # 1. Test Hash Map point lookup
    ops_to_run_hash = 10000
    print(f"\n--- 1. SELECT O(1) Hash Map Index Hit (Running {ops_to_run_hash:,} ops) ---")
    q1 = f"SELECT * FROM BENCHMARK WHERE ID = {num_rows - 1};"
    start_time = time.time()
    for i in range(ops_to_run_hash):
        send_query(s, q1)
    end_time = time.time()
    hash_duration = end_time - start_time
    print(f"Time Taken ({ops_to_run_hash:,} Primary Key Lookups): {hash_duration:.4f} seconds")
    print(f"--> SELECT (O(1) Hash Map) Throughput: {ops_to_run_hash / hash_duration:,.2f} operations/sec")

    # 2. Test Full Column Scan (Unindexed column search forces iterating the std::vector)
    ops_to_run_scan = 1000
    print(f"\n--- 2. SELECT Full 'Column-Major' Vector Scan (Running {ops_to_run_scan:,} ops) ---")
    q2 = f"SELECT * FROM BENCHMARK WHERE VAL = 'Data_{num_rows - 1}';"
    
    # We must explicitly disable the LRU Cache for THIS Specific test by spamming slightly slightly different queries, 
    # otherwise our massive LRU Cache will perfectly intercept them natively!
    start_time = time.time()
    for i in range(ops_to_run_scan):
        send_query(s, f"SELECT * FROM BENCHMARK WHERE VAL = 'Data_{num_rows - 1 - i}';")
    end_time = time.time()
    scan_duration = end_time - start_time
    print(f"Time Taken ({ops_to_run_scan:,} Full Unindexed Vector Scans over {num_rows:,} rows): {scan_duration:.4f} seconds")
    print(f"--> SELECT (Full Scan) Throughput: {ops_to_run_scan / scan_duration:,.2f} operations/sec")
    
    # 3. Test LRU Cache 
    ops_to_run_lru = 50000
    print(f"\n--- 3. SELECT O(1) LRU Cache Hit (Running {ops_to_run_lru:,} ops) ---")
    start_time = time.time()
    # Sending the exact same query geometrically guarantees the LRU hits
    for i in range(ops_to_run_lru):
        send_query(s, q2)
    end_time = time.time()
    lru_duration = end_time - start_time
    print(f"Time Taken ({ops_to_run_lru:,} LRU Cache bypass ops): {lru_duration:.4f} seconds")
    print(f"--> SELECT (LRU Cache) Throughput: {ops_to_run_lru / lru_duration:,.2f} operations/sec")
    
    s.close()
    print("\nBenchmark Complete.")

if __name__ == "__main__":
    benchmark()
