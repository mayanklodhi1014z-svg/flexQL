with open("src/parser.cpp", "r") as f:
    text = f.read()

# Replace:
# size_t join_pos = q_upper.find(" INNER JOIN ");
# with:
replacement = """
        size_t join_pos = q_upper.find(" INNER JOIN ");
        int join_len = 12;
        if (join_pos == std::string::npos) {
            join_pos = q_upper.find(" JOIN ");
            join_len = 6;
        }
        size_t where_pos = q_upper.find(" WHERE ");

        if (join_pos != std::string::npos) {
            size_t on_pos = q_upper.find(" ON ", join_pos);
            if (on_pos != std::string::npos) {
                size_t tb_end = q_upper.find(" ", join_pos + join_len);
                if (tb_end > on_pos) tb_end = on_pos;
                node.join_table = q.substr(join_pos + join_len, tb_end - (join_pos + join_len));
"""

import re
text = re.sub(
    r'size_t join_pos = q_upper\.find\(" INNER JOIN "\);\s*size_t where_pos = q_upper\.find\(" WHERE "\);\s*if \(join_pos != std::string::npos\) \{\s*size_t on_pos = q_upper\.find\(" ON ", join_pos\);\s*if \(on_pos != std::string::npos\) \{\s*size_t tb_end = q_upper\.find\(" ", join_pos \+ 12\);\s*if \(tb_end > on_pos\) tb_end = on_pos;\s*node\.join_table = q\.substr\(join_pos \+ 12, tb_end - \(join_pos \+ 12\)\);',
    replacement, text
)

with open("src/parser.cpp", "w") as f:
    f.write(text)

