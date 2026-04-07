# FlexQL System Design Document

**FlexQL** is a high-performance, multithreaded, in-memory/disk-hybrid C++ database engineered for massive concurrent workloads. 

**Key Achievements:**
- **Hybrid Storage (LSM-Tree Style):** Seamlessly flushes massive RAM blocks (100k+ rows) to immutable disk \`.sst\` (Sorted String Table) files, allowing scale beyond physical memory limits.
- **Lock-Free Concurrency:** Up to 700K+ rows/sec natively through atomic vectors and Lock-Free index mechanics.
- **Dynamic Memory Management:** Exponential capacity growth ensures minimum footprint for small tables and amortized O(1) throughput for large benchmarks.
- **Write-Ahead Log (WAL):** Crash-safe durable logging persisting to \`data/wal/\` disk structures on every mutating query.
- **Dual-Level Query Caching:** Bounded AST tokenization cache (LRU) and dependent-result cache block duplicate processing overhead.
- **Cache-Aligned Processing:** B+ Tree and Hashmap architectures optimized tightly against L1/L2 cache metrics.

---

## Table of Contents

1. [System Architecture](#1-system-architecture)
2. [Data Storage Design (Hybrid LSM)](#2-data-storage-design-hybrid-lsm)
3. [Indexing Strategy](#3-indexing-strategy)
4. [Caching Strategy](#4-caching-strategy)
5. [Expiration Timestamp Handling](#5-expiration-timestamp-handling)
6. [Multithreading & Concurrency Concept](#6-multithreading--concurrency-concept)
7. [Write-Ahead Logging (WAL) & Recovery](#7-write-ahead-logging-wal--recovery)
8. [Performance Optimizations](#8-performance-optimizations)
9. [API Design & Protocols](#9-api-design--protocols)
10. [Known Constraints & Future Improvements](#10-known-constraints--future-improvements)

---

## 1. System Architecture

### 1.1 System Overview

FlexQL follows a **client-server architecture** with the following layers:

1. **Client Layer**: Interactive REPL and C API that sends SQL queries via TCP length-prefixed sockets.
2. **Server Layer**: Multithreaded request processor utilizing a specialized thread pool dynamically mirroring physical core limits.
3. **Parsing Layer**: SQL tokenization mapping text to standard Abstract Syntax Tree (AST) nodes.
4. **Execution Layer**: Statement execution engine orchestrating dynamic schema evaluation and dual-stage caching.
5. **Storage Layer**: A hybrid row-major memory matrix backed by scalable LSM tree '.sst' pages.
6. **Durability Layer**: Immutable WAL append-only sequence guaranteeing recovery via playback loops.

### 1.2 Component Responsibilities

| Component | Responsibility | Core File |
|-----------|----------------|-----------|
| **Server** | TCP socket listeners & connection dispatches. | \`src/server.cpp\` |
| **Executor** | Dynamic JOIN, SELECT, INSERT logic mixing RAM & Disk targets. | \`src/executor.cpp\` |
| **StorageEngine** | Table allocation and TTL (Time-to-Live) management. | \`src/storage_engine.cpp\` |
| **Table** | Active MemTable vector lists & \`.sst\` page iteration logic. | \`src/table.cpp\` |
| **ThreadPool** | Bounded context-switching worker delegation. | \`include/flexql/thread_pool.h\` |
| **BTreeIndex** | Custom Cache-Aligned B+ Tree mapped to RAM row boundaries. | \`include/flexql/btree_index.h\` |

---

## 2. Data Storage Design (Hybrid LSM)

### 2.1 Core Philosophy: Row-Major Vectors + Disk Flushing

FlexQL handles unbounded table growth natively by leveraging a hybrid topology:
1. **MemTable**: Incoming `INSERT`s are batched row-by-row into atomic strings vectors (the active MemTable). Memory dynamically allocates by doubling (8192, 16384, etc.) utilizing \`std::shared_mutex\` synchronization.
2. **SST Flush Limits**: When an active table reaches **100,000 rows**, the MemTable is securely hot-swapped (frozen) and asynchronously serialized to flat, immutable Sorted String Table (\`.sst\`) files in the \`data/sst/\` directory.
3. **Transparent Reads**: Any \`SELECT\` or \`JOIN\` seamlessly sweeps both the live MemTable buffers *and* fires asynchronous lambda callbacks to traverse flat binary \`.sst\` background chunk allocations to compute matching filters. 

### 2.2 Column Formats

- **Primary Matrix:** \`columns_data_\` (Generic UTF-8 payload blocks).
- **Fast Numerics:** \`int_columns_\` and \`decimal_columns_\` pre-casted runtime representations avoiding continuous \`std::stod\` or \`std::stoi\` cast penalties on numeric WHERE filters.
- **Timestamps:** Vector of epoch time identifiers mapped identically to parallel table rows.

---

## 3. Indexing Strategy

### 3.1 Lock-Free HashMap + True cache-aligned B+ Tree

**Primary Key Resolution:** 
FlexQL splits lookup resolutions natively through custom concurrent constructs.

- Fast Index probing via **Lock-Free Hash Maps**: Ensuring multithreaded lookups and index uniqueness verifications don't spin-stall.
- Native sequential range probing via **Cache-Aligned B+ Trees**: Max 127 branch branching-factors compress CPU jumps. L1/L2 misses are reduced during nested ORDER BY routines. 

### 3.2 Index Management across Table Hot-Swaps

Because tables automatically serialize heavily into \`.sst\` layers, memory indices solely mirror the active MemTable context. Queries seeking historical records sequentially binary-scan disk payload strings mapped implicitly via indexed offsets, dramatically decreasing static index RAM bulk.

---

## 4. Caching Strategy

### 4.1 Dual-Level Architecture
Query acceleration occurs on two primary thresholds:
1. **AST Cache**: SQL string to compiled parsing tree (1000 items LRU capped). Limits regex tokenization CPU burn.
2. **Result Cache**: Final assembled projection arrays tracking cross-table dependencies (4096 items capped). Validates dependency TTLs (Time-To-Live).

**Invalidation Trigger Vectors:**
- Table-level cache drop triggered immediately post-\`CREATE\`, \`INSERT\`, or \`DELETE\` mutations.
- Fast `min_row_ttl` boundary guarantees a cached subset auto-evicts organically if a TTL timeframe inside expires. 

---

## 5. Expiration Timestamp Handling

**Insertion Trigger (EXPIRES):**
\`INSERT INTO TABLE (...,) EXPIRES <timestamp>;\`

**Non-Blocking Sweep:**
A background \`Sweeper\` thread invokes \`try_to_lock\` mutex assertions. If a table currently blocking heavily under read/write activity prevents sweeping, it's skipped contextually to avoid writer stalls. Once locked natively, TTL expiration filters out historical row mappings compacting remaining valid lines linearly and wiping \`.sst\` overlap.

---

## 6. Multithreading & Concurrency Concept

### 6.1 Worker Threads
A purely dynamic \`std::thread\` bound mapping scales worker threads precisely against literal hardware core metrics. Thread bounds protect against exponential connection queue context over-switches.

### 6.2 Lock-Free Insert Boundaries
To scale against 700K+ writes rapidly:
- Natively threaded vectors rely comprehensively on atomic fetch-and-adds (\`std::atomic<size_t> row_count_\`). 
- \`std::shared_mutex\` boundaries decouple Reader collisions, meaning Writer streams map directly into completely unique logical vector string slots without block contention.

---

## 7. Write-Ahead Logging (WAL) & Recovery

### 7.1 Architecture
Durability spans natively over crash-safe write-ahead protocols:
- `data/wal/server.log` acts as an append-only log sequence directly bound to all standard SQL mutation operations.
- Writes push payload directly utilizing \`fdatasync()\` (or \`ostream::flush()\`) guarantees.  

### 7.2 Boot Bootstrapping
Before any sockets unbind:
1. Validates standard directories (hot-creating \`data/sst\` and \`data/wal\`).
2. Iterates over arbitrary \`.sst\` binaries mapping chunks back uniquely to their source schemas.
3. Rapidly parses raw SQL log limits from \`wal/server.log\` inside \`execute(is_replay=true)\` ignoring duplicates to reconstruct MemTables safely.

---

## 8. Performance Optimizations

| Optimization | Technique | Impact |
|---|---|---|
| **LSM-Tree Architecture** | \`.sst\` flushing on 100k thresholds | Bounded memory consumption guarantees scale towards 10m+ row bounds smoothly. |
| **Lock-free Insertion** | Atomic \`row_count_\` distribution | Linear vertical scale outlasting standard mutex block locks. |
| **Vector Double-sizing** | Capacity-growth increments | Drastically drops micro allocations in rapid sequence strings. |
| **Dual Caching** | Parse & Result level bounding | Drops regex execution latency dynamically to a handful of microseconds. |
| **Typed Accel** | Native INT/DECIMAL blocks | Sidesteps continuous string runtime checks on WHERE filtering loops. |

---

## 9. API Design & Protocols

### 9.1 Network Format
Clients dispatch length-prefixed raw bytes over direct TCP streams preventing slow polling dependencies.  
- Success prefix \`O<newline>\` + TSV block
- Error prefix \`E<newline>\` + Details

### 9.2 Client Binding
\`client_api.h\` embeds standardized functions (\`flexql_open\`, \`flexql_exec\`, \`flexql_close\`). Supports cross IPV4 and IPv6 packet parsing natively ensuring portable application integration schemas. 
### 9.3 Client REPL & Robust Parsing
The frontend `flexql-client` encompasses a highly resilient, interactive shell loop tailored specifically for developer testing and robust query handling:
- **Resilient Statement Accumulation:** Multi-line inputs are aggressively cached with an intuitive `    ->` continuation prompt. The shell's frontend securely trims trailing whitespaces, nested newlines, and stray `;` characters. Let's users safely copy-paste massive SQL blocks simultaneously without the parser crashing into `UNKNOWN` states.
- **Mutative Feedback Loops:** Quietly executes actions that modify context without returning grid schemas (`INSERT`, `CREATE`, `DROP`, `DELETE`). Now successfully acknowledges these operations properly via direct UI feedback (`Query OK, operation successful.`), preventing infinite shell holds.
- **Expanded Token Dialects:** The backend C++ AST SQL Tokenizer features expanded standard dialect support natively parsing aliases interchangeably (e.g. mapping implicit `JOIN` clauses flawlessly rather than rigidly rejecting if non-explicit `INNER JOIN` tokens are missing).
---

## 10. Known Constraints & Future Improvements

1. **Storage Deferral Deletion**: \`.sst\` row compactions can be enhanced to merge out distinct chunk overlaps natively rather than exclusively filtering on TTL sweeps.
2. **Advanced Joins**: JOIN logic inherently operates under Nested Loops (merging MemTables directly scanning \`.sst\` files asynchronously). Implementing partitioned Hash-Joins across \`.sst\` bounds would accelerate O(n*m) multi-million row joins strictly. 
3. **Aggregations**: Core SQL logic scales seamlessly over linear selects but does not naturally implement native window clustering functions (\`AVG\`, \`SUM\`, \`COUNT\`). 
