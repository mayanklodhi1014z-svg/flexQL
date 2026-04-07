void Table::flushToSSTable(const std::string& directory) {
    // Acquire exclusive write lock to freeze the MemTable
    std::unique_lock<std::shared_mutex> wl(table_mutex_);

    size_t rows_to_flush = row_count_.load();
    if (rows_to_flush == 0) return; // Nothing to flush

    // 1. Swap current arrays with empty arrays
    auto old_columns_data = std::move(columns_data_);
    auto old_int_columns = std::move(int_columns_);
    auto old_int_nulls = std::move(int_nulls_);
    auto old_decimal_columns = std::move(decimal_columns_);
    auto old_decimal_nulls = std::move(decimal_nulls_);
    auto old_expirations = std::move(expiration_timestamps_);

    // 2. Reset table state seamlessly
    columns_data_.resize(schema_.size());
    int_columns_.resize(schema_.size());
    int_nulls_.resize(schema_.size());
    decimal_columns_.resize(schema_.size());
    decimal_nulls_.resize(schema_.size());
    expiration_timestamps_.clear();
    row_count_.store(0);
    current_capacity_.store(0);

    // Track the new SST file path immediately so that SELECT reads it
    // Use an atomic or protected list? sst_files_ can be protected by table_mutex_
    std::string filename = directory + "/" + name_ + "_" + std::to_string(time(nullptr)) + "_" + std::to_string((unsigned long long)this) + ".sst";
    sst_files_.push_back(filename);

    wl.unlock(); // Readers/Writers can immediately resume on new MemTable!

    // 4. Background serialization of the old arrays into an SSTable
    std::thread([this, filename, rows_to_flush, 
        cols = std::move(old_columns_data),
        exps = std::move(old_expirations)]() {
        
        std::ofstream out(filename, std::ios::binary);
        if (!out) {
            std::cerr << "Failed to create SSTable: " << filename << std::endl;
            return;
        }

        // Write row count simply as text or binary (let's do binary for speed)
        out.write(reinterpret_cast<const char*>(&rows_to_flush), sizeof(rows_to_flush));
        
        // Detailed serialization of columns row-by-row
        size_t num_cols = schema_.size();
        for (size_t r = 0; r < rows_to_flush; ++r) {
            for (size_t c = 0; c < num_cols; ++c) {
                const std::string& val = cols[c][r];
                size_t len = val.size();
                out.write(reinterpret_cast<const char*>(&len), sizeof(len));
                if (len > 0) {
                    out.write(val.data(), len);
                }
            }
            time_t expire = exps[r];
            out.write(reinterpret_cast<const char*>(&expire), sizeof(expire));
        }
        
        out.close();
        std::cout << "Flushed " << rows_to_flush << " rows to " << filename << std::endl;
        
    }).detach();
}

void Table::scanSSTables(const std::function<void(const std::vector<std::string>& row_data, time_t expiration)>& callback) const {
    size_t num_cols = schema_.size();
    
    // table_mutex_ should already be held securely as a shared_lock by Executor::executeSelect!
    for (const std::string& filename : sst_files_) {
        std::ifstream in(filename, std::ios::binary);
        if (!in) continue;
        
        size_t rows;
        if (!in.read(reinterpret_cast<char*>(&rows), sizeof(rows))) continue;
        
        std::vector<std::string> row_data(num_cols);
        for (size_t r = 0; r < rows; ++r) {
            for (size_t c = 0; c < num_cols; ++c) {
                size_t len;
                if (!in.read(reinterpret_cast<char*>(&len), sizeof(len))) break;
                if (len > 0) {
                    row_data[c].resize(len);
                    in.read(&row_data[c][0], len);
                } else {
                    row_data[c].clear();
                }
            }
            time_t expire;
            if (!in.read(reinterpret_cast<char*>(&expire), sizeof(expire))) break;
            
            callback(row_data, expire);
        }
    }
}
} // namespace flexql
