#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
// | startId | startLabel | edgeLabel | endId | endLabel |

constexpr char sep = '.';
constexpr int MAXBUFREAD = 4096;

char labelBuf[512];
std::vector<std::string_view> labels;
int labelLen = 0;
char resultBuf[512];
std::vector<std::string_view> relations;
int relationLen = 0;

int process_csv(char* filename) {
    auto fp = fopen(filename, "r");
    if (!fp) {
        perror("fopen");
        return 1;
    }
    char buf[MAXBUFREAD];
    int len = MAXBUFREAD;
    std::string_view cols[5];
    size_t offset = 0;
    while (1) {
        size_t size = fread(buf + offset, 1, len, fp);
        if (size <= 0) [[unlikely]] {
            break;
        }
        std::string_view chunk(buf, size);
        size_t start = 0;
        size_t pos = chunk.find('\n', start);
        if (pos != std::string_view::npos) {
            start = pos += 1;
            pos = chunk.find('\n', start);
            std::string_view line(buf + start, pos);
            setFile(cols, line);
        }
        if (size + offset == MAXBUFREAD) [[likely]] {
            memcpy(buf, buf + start, size - start);
            len = MAXBUFREAD - (size - start);
            continue;
        }
        if (start < size + offset) {
            setFile(cols, std::string_view(buf + start, size + offset - start));
        }
        break;
    }

    if (feof(fp)) {
        return 0;
    } else if (ferror(fp)) {
        perror("Error reading file");
    }
}

void setFile(std::string_view* p, std::string_view line) {

    size_t index = line.find(',');
    p[0] = std::string_view(line.data(), index);
    size_t index2 = line.find(',', index + 1);
    p[1] = std::string_view(line.data() + index + 1, index2 - index - 1);
    index = index2;
    index2 = line.find(',', index + 1);
    p[2] = std::string_view(line.data() + index + 1, index2 - index - 1);
    index = index2;
    index2 = line.find(',', index + 1);
    p[3] = std::string_view(line.data() + index + 1, index2 - index - 1);
    p[4] = std::string_view(line.data() + index2 + 1, line.end());
}

void setLabels(std::string_view* p) {
    bool flag = true;
    int l1 = 0;
    for (int i = 0; i < labels.size(); i++) {
        if (labels[i] == p[1]) {
            flag = false;
            l1 = i;
        }
    }
    if (flag) {
        l1 = labels.size();
        int offset = labelLen;
        memcpy(labelBuf + offset, p[1].data(), p[1].size());

        labels.push_back() labels.push_back(p[1]);
    }
    int l2 = 0;
    flag = true;
    for (int i = 0; i < labels.size(); i++) {
        if (labels[i] == p[4]) {
            flag = false;
            l1 = i;
        }
    }
    if (flag) {
        l2 = labels.size();
        labels.push_back(p[4]);
    }
r3:
    =
}

int processData(char* row_buf, int len, bool end) {
    char* buf = row_buf;
    bool flag = false;
    for (;;) {
        std::string_view line(buf, len);
        auto lineIndex = line.find('\n');
        if (lineIndex == std::string_view::npos && end) [[unlikely]] {
            memcpy(row_buf, line.data(), len);
            return len;
        }
        std::string_view linestr(buf, lineIndex);
        auto index1 = linestr.find(',');
        auto index2 = linestr.find(',', index1 + 1);
        auto index3 = linestr.find(',', index2 + 1);
        auto index4 = linestr.find(',', index3 + 1);
        auto index5 = linestr.find(',', index4 + 1);
        buf += lineIndex + 1;
        len -= lineIndex + 1;
        if (flag) [[unlikely]] {
            return 0;
        }
    }
}