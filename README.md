# FlexQL

FlexQL is an optimized, high-performance C++ Database Engine and Client-Server architecture featuring a custom cache-aligned B+ Tree for indexing.

## Compile Instructions

The project uses CMake and Make for building the required binaries. To compile the project, run the following commands from the root directory:

```bash
# Build all components (Client, Server, and Benchmarks)
make build

# Alternatively, you can use raw CMake directly:
mkdir -p build && cd build && cmake .. && make
```

Available Make targets:
* `make build`: Bootstraps CMake and builds all targets into the `bin/` directory.
* `make server`: Builds only the server.
* `make client`: Builds only the client.
* `make clean`: Removes the `build/` and `bin/` directories.

## Run Commands

Compiled binaries are placed in the `bin/` directory.

### 1. Start the Server
Start the database server first. By default, it runs on `127.0.0.1` and listens on port `9000`.

```bash
./bin/flexql-server
```
*Optional formatting:* `./bin/flexql-server <host> <port>` (e.g., `./bin/flexql-server 127.0.0.1 9000`)

### 2. Run the Client
Once the server is running, use the client application to connect and execute SQL commands.

```bash
./bin/flexql-client
```

### 3. Run Benchmark Tests
To profile performance or run the system benchmark suite which executes native inserts and full SQL query operations, supply the number of rows:

```bash
# Example: Run benchmark with 1,000,000 rows
./bin/benchmark_flexql 1000000
```
*(By default, this dumps output to stdout. You can also redirect to a file: `./bin/benchmark_flexql 10000000 > curr_otp.txt`)*

## Server Port Management (Clearing a Busy Port)

If the server crashes or you get a `"Address already in use"` (bind failed) error when starting the server, it means port `9000` is still busy. You can clear the port using any of these helpful terminal commands:

**Option 1: Using `fuser` (Fastest)**
Kills any processes listening on port 9000:
```bash
fuser -k 9000/tcp
```

**Option 2: Using `lsof`**
Finds the Process ID (PID) holding port 9000 and forcefully kills it:
```bash
kill -9 $(lsof -t -i:9000)
```

**Option 3: Using `pkill`**
Terminates all running instances of the `flexql-server` binary globally:
```bash
pkill -9 -f flexql-server
```

## Useful Information & Architecture

* **Primary Key Index**: Uses a True Cache-Aligned B+ Tree engineered to strictly reduce CPU RAM stalls and maximize spatial locality (L1/L2 cache hits) for high-throughput range (`ORDER BY`) and point lookups.
* **Storage Engine**: Located in `src/storage_engine.cpp`, it organizes rows in structures engineered for multi-threading.
* **Execution Engine**: `src/executor.cpp` executes the AST (Abstract Syntax tree) with rich operator evaluation and complex join handling.
* **Testing / Debugging**: Use the `data/wal/` directory if analyzing Write-Ahead-Log recovery states.

For an extensive dive into the performance profiling, indexing rationale, and threading architecture, read the **[DESIGN_DOCUMENT.md](DESIGN_DOCUMENT.md)** and the **[PERFORMANCE_OPTIMIZATION_STRATEGY.md](PERFORMANCE_OPTIMIZATION_STRATEGY.md)**.
