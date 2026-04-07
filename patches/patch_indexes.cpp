    primary_index_.clear();
    for (auto* idx : int_range_indexes_) { if (idx) idx->clear(); }
    for (auto* idx : decimal_range_indexes_) { if (idx) idx->clear(); }
