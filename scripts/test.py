import socket
import struct
import time

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

def test_db():
    s = socket.socket()
    try:
        s.connect(('127.0.0.1', 9000))
    except Exception as e:
        print("Server not running:", e)
        return

    print("--- CREATE TABLE ---")
    print(send_query(s, "CREATE TABLE STUDENT(ID INT PRIMARY KEY, NAME VARCHAR);"))

    print("--- INSERT ---")
    print(send_query(s, "INSERT INTO STUDENT VALUES(1, 'Alice');"))
    # Expire in 2 seconds
    expire_ts = int(time.time()) + 2
    print(send_query(s, f"INSERT INTO STUDENT VALUES(2, 'Bob') EXPIRES {expire_ts};"))
    
    print("--- SELECT ALL ---")
    print(send_query(s, "SELECT * FROM STUDENT;"))

    print("--- SELECT WITH INDEX ---")
    print(send_query(s, "SELECT * FROM STUDENT WHERE ID = 1;"))

    print("\nWaiting 3 seconds for Bob to expire...\n")
    time.sleep(3)

    print("--- SELECT ALL (Post-Expiration) ---")
    print(send_query(s, "SELECT * FROM STUDENT;"))

    s.close()
    print("Done")

if __name__ == "__main__":
    test_db()
