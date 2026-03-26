#!/usr/bin/env python3
import argparse
import random
import socket
import string
import struct
import time


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


def parse_response(resp: str):
	lines = resp.split("\n")
	if not lines or lines[0] == "E":
		msg = lines[1] if len(lines) > 1 else "unknown error"
		return False, msg, []
	# OK path: O, header, --- , rows
	rows = []
	if len(lines) >= 3:
		for line in lines[3:]:
			if not line:
				continue
			rows.append(line.split("\t"))
	return True, "", rows


def random_value(length: int = 8) -> str:
	alphabet = string.ascii_lowercase
	return "".join(random.choice(alphabet) for _ in range(length))


def bench(host: str, port: int, rows: int, batch: int, lookups: int):
	sock = socket.create_connection((host, port))
	sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

	def exec_sql(sql: str):
		send_frame(sock, sql)
		return recv_frame(sock)

	# Drop table to avoid piling on prior runs
	ok, msg, _ = parse_response(exec_sql("DROP TABLE IF EXISTS bench"))
	if not ok:
		raise RuntimeError(f"DROP failed: {msg}")

	# Create table
	resp = exec_sql("CREATE TABLE bench (id INT PRIMARY KEY, name VARCHAR)")
	ok, msg, _ = parse_response(resp)
	if not ok and "already exists" not in msg:
		raise RuntimeError(f"CREATE failed: {msg}")

	# Inserts
	start = time.perf_counter()
	next_log = 0
	for start_id in range(0, rows, batch):
		end_id = min(start_id + batch, rows)
		vals = ",".join(f"({i},'{random_value(8)}')" for i in range(start_id, end_id))
		resp = exec_sql(f"INSERT INTO bench VALUES {vals}")
		ok, msg, _ = parse_response(resp)
		if not ok:
			raise RuntimeError(f"INSERT failed: {msg}")
		done = end_id
		if done >= next_log:
			elapsed = time.perf_counter() - start
			rate = done / elapsed if elapsed > 0 else 0
			print(f"Inserted {done}/{rows} rows ({rate:,.0f} rows/s)", flush=True)
			next_log += max(batch * 10, 10000)
	insert_time = time.perf_counter() - start

	# PK lookups
	rng = random.Random(42)
	lookup_ids = [rng.randrange(0, rows) for _ in range(lookups)]
	start = time.perf_counter()
	for i in lookup_ids:
		sql = f"SELECT * FROM bench WHERE id = {i}"
		exec_sql(sql)
	lookup_time = time.perf_counter() - start

	# Full scan
	start = time.perf_counter()
	resp = exec_sql("SELECT * FROM bench")
	ok, msg, rows_out = parse_response(resp)
	scan_time = time.perf_counter() - start
	if not ok:
		raise RuntimeError(f"SCAN failed: {msg}")

	sock.close()

	print(f"\n=== FlexQL Benchmark Results ===")
	print(f"Inserts: {rows} rows in {insert_time:.3f}s => {rows/insert_time:,.0f} rows/s")
	print(f"Lookups: {lookups} queries in {lookup_time:.3f}s => {lookups/lookup_time:,.0f} qps")
	print(f"Full scan: {len(rows_out)} rows in {scan_time:.3f}s")


def main():
	parser = argparse.ArgumentParser(description="FlexQL benchmark (client-side)")
	parser.add_argument("--host", default="127.0.0.1")
	parser.add_argument("--port", type=int, default=9000)
	parser.add_argument("--rows", type=int, default=100000, help="number of rows to insert")
	parser.add_argument("--batch", type=int, default=500, help="rows per INSERT batch")
	parser.add_argument("--lookups", type=int, default=10000, help="number of PK lookups")
	args = parser.parse_args()

	bench(args.host, args.port, args.rows, args.batch, args.lookups)


if __name__ == "__main__":
	main()
