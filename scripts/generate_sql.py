#!/usr/env/bin python3
import sys
import random
import string

def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_file(rows, filename):
    print(f"Generating {rows} insertion commands into '{filename}'...")
    
    with open(filename, 'w') as f:
        f.write("DROP TABLE IF EXISTS bench\n")
        f.write("CREATE TABLE bench (id INT PRIMARY KEY, val VARCHAR)\n")
        
        # We batch 1,000 values into a single INSERT string line.
        # This prevents the TCP terminal from doing 1-million single-ping loops.
        batch_size = 1000
        for b in range(0, rows, batch_size):
            end = min(b + batch_size, rows)
            vals = []
            for i in range(b, end):
                vals.append(f"({i},'{random_string()}')")
            
            f.write(f"INSERT INTO bench VALUES {','.join(vals)}\n")
            
    print("Generation complete! You can now pipe this file to the client.")

if __name__ == "__main__":
    rows = int(sys.argv[1]) if len(sys.argv) > 1 else 1000000
    fname = sys.argv[2] if len(sys.argv) > 2 else "test_data.sql"
    generate_file(rows, fname)
