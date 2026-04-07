import struct
import sys

if len(sys.argv) < 2:
    print("Usage: python3 read_sst.py <file.sst> [num_cols]")
    sys.exit(1)

filename = sys.argv[1]
# BIG_USERS has 5 columns
num_cols = int(sys.argv[2]) if len(sys.argv) > 2 else 5

with open(filename, "rb") as f:
    rows_bytes = f.read(8)
    if not rows_bytes:
        sys.exit(0)
    rows_to_flush = struct.unpack("Q", rows_bytes)[0]
    print(f"=== SSTable Contents ({filename}) ===")
    print(f"Total Rows: {rows_to_flush}\n")

    for r in range(min(rows_to_flush, 15)): # Print first 15 rows for preview
        row_data = []
        for c in range(num_cols):
            len_bytes = f.read(8)
            if not len_bytes:
                break
            val_len = struct.unpack("Q", len_bytes)[0]
            if val_len > 0:
                val = f.read(val_len).decode('utf-8', errors='replace')
            else:
                val = "NULL"
            row_data.append(val)
        
        exp_bytes = f.read(8)
        if not exp_bytes:
            break
        expire = struct.unpack("q", exp_bytes)[0]
        
        print(f"Row {r + 1}: {row_data}")
        
    if rows_to_flush > 15:
        print(f"\n... and {rows_to_flush - 15} more rows.")
