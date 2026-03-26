#!/usr/env/bin python3
import socket
import struct
import json

def send_frame(sock, sql):
    data = sql.encode()
    hdr = struct.pack("!I", len(data))
    sock.sendall(hdr + data)

def recv_frame(sock):
    hdr = sock.recv(4)
    if len(hdr) < 4: return "{}"
    (length,) = struct.unpack("!I", hdr)
    buf = bytearray()
    while len(buf) < length:
        buf.extend(sock.recv(length - len(buf)))
    return buf.decode()

def exec_sql(sock, sql):
    print(f"\n[Query] {sql}")
    send_frame(sock, sql)
    resp = recv_frame(sock)
    try:
        parsed = json.loads(resp)
        print(json.dumps(parsed, indent=2))
    except:
        print(resp)

def test_joins():
    print("=== Testing FlexQL INNER JOIN ===")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 9001))
    
    # 1. Setup USERS table
    exec_sql(s, "DROP TABLE IF EXISTS users")
    exec_sql(s, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)")
    exec_sql(s, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
    
    # 2. Setup ORDERS table
    exec_sql(s, "DROP TABLE IF EXISTS orders")
    exec_sql(s, "CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, amount VARCHAR)")
    exec_sql(s, "INSERT INTO orders VALUES (101, 1, '$50.00'), (102, 1, '$120.00'), (103, 2, '$10.00')")
    
    # 3. Test INNER JOIN
    exec_sql(s, "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.id = 1")
    exec_sql(s, "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.id = 2")
    exec_sql(s, "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.id = 3")

if __name__ == "__main__":
    test_joins()
