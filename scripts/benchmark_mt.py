#!/usr/env/bin python3
import argparse
import random
import socket
import string
import struct
import time
import threading
from concurrent.futures import ThreadPoolExecutor

# Global counters for live throughput monitoring
total_inserts = 0
total_lookups = 0
monitor_running = True

def throughput_monitor():
    global total_inserts, total_lookups, monitor_running
    last_inserts = 0
    last_lookups = 0
    while monitor_running:
        time.sleep(1.0)
        curr_i = total_inserts
        curr_l = total_lookups
        i_speed = curr_i - last_inserts
        l_speed = curr_l - last_lookups
        last_inserts = curr_i
        last_lookups = curr_l
        
        if i_speed > 0:
            print(f"   Inserts: {curr_i:10,} rows | Speed: {i_speed:8,} rows/sec")
        if l_speed > 0:
            print(f"  Lookups: {curr_l:10,} done  | Speed: {l_speed:8,} qps")

def send_frame(sock: socket.socket, sql: str) -> None:
    data = sql.encode()
    hdr = struct.pack("!I", len(data))
    sock.sendall(hdr + data)

def recv_frame(sock: socket.socket) -> str:
    hdr = sock.recv(4)
    if len(hdr) < 4:
        raise RuntimeError("connection closed")
    (length,) = struct.unpack("!I", hdr)
    buf = bytearray()
    while len(buf) < length:
        chunk = sock.recv(length - len(buf))
        if not chunk:
            raise RuntimeError("connection closed mid-frame")
        buf.extend(chunk)
    return buf.decode()

def exec_sql(sock, sql: str) -> str:
    send_frame(sock, sql)
    return recv_frame(sock)

def random_value(length: int = 8) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))

def insert_worker(host, port, start_id, end_id, batch_size):
    global total_inserts
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.connect((host, port))
    
    # Pre-build a massive generic values array to bypass Python's slow GIL string formatting overhead entirely
    generic_val = "'abcd123'"
    
    t0 = time.perf_counter()
    for batch_start in range(start_id, end_id, batch_size):
        batch_end = min(batch_start + batch_size, end_id)
        
        # Fast python comprehension
        vals = ",".join(f"({i},{generic_val})" for i in range(batch_start, batch_end))
        sql = f"INSERT INTO bench VALUES {vals}"
        
        exec_sql(sock, sql)
        total_inserts += (batch_end - batch_start)
    
    t1 = time.perf_counter()
    return end_id - start_id, t1 - t0

def lookup_worker(host, port, ids):
    global total_lookups
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.connect((host, port))
    
    t0 = time.perf_counter()
    batch_count = 0
    for i in ids:
        exec_sql(sock, f"SELECT * FROM bench WHERE id = {i}")
        batch_count += 1
        if batch_count >= 100:
            total_lookups += batch_count
            batch_count = 0
            
    if batch_count > 0:
        total_lookups += batch_count
        
    t1 = time.perf_counter()
    return len(ids), t1 - t0

def bench(host: str, port: int, rows: int, batch: int, lookups: int, threads: int):
    global monitor_running
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    exec_sql(s, "DROP TABLE IF EXISTS bench")
    exec_sql(s, "CREATE TABLE bench (id INT PRIMARY KEY, val VARCHAR)")
    
    print(f"\n=== Multithreaded FlexQL Benchmark ({threads} Threads) ===")

    # Start live monitor thread
    monitor_thread = threading.Thread(target=throughput_monitor, daemon=True)
    monitor_thread.start()

    # 1. Multi-threaded Inserts
    print(f"\nStarting {rows:,} Inserts...")
    start_time = time.perf_counter()
    
    chunk_size = rows // threads
    futures = []
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for t in range(threads):
            start_id = t * chunk_size
            end_id = rows if t == threads - 1 else (t + 1) * chunk_size
            futures.append(executor.submit(insert_worker, host, port, start_id, end_id, batch))
    
    sum_inserts = sum(f.result()[0] for f in futures)
    end_time = time.perf_counter()
    insert_sec = end_time - start_time
    print(f"\n[DONE] Inserts: {sum_inserts:,} rows in {insert_sec:.3f}s => {sum_inserts/insert_sec:,.0f} rows/s")

    # 2. Multi-threaded Lookups
    print(f"\nStarting {lookups:,} Lookups...")
    all_lookup_ids = [random.randint(0, rows - 1) for _ in range(lookups)]
    lookup_chunk = lookups // threads
    
    start_time = time.perf_counter()
    futures = []
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for t in range(threads):
            start_idx = t * lookup_chunk
            end_idx = lookups if t == threads - 1 else (t + 1) * lookup_chunk
            futures.append(executor.submit(lookup_worker, host, port, all_lookup_ids[start_idx:end_idx]))
    
    sum_lookups = sum(f.result()[0] for f in futures)
    end_time = time.perf_counter()
    lookup_sec = end_time - start_time
    print(f"\n[DONE] Lookups: {sum_lookups:,} queries in {lookup_sec:.3f}s => {sum_lookups/lookup_sec:,.0f} qps")

    # Stop live monitor
    monitor_running = False
    monitor_thread.join(timeout=1.0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--batch", type=int, default=10000)
    parser.add_argument("--lookups", type=int, default=1000000)
    parser.add_argument("--threads", type=int, default=8)
    args = parser.parse_args()
    bench(args.host, args.port, args.rows, args.batch, args.lookups, args.threads)
