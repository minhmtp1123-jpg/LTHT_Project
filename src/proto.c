// proto.c
#include <stdint.h>
#include <arpa/inet.h>
#include <errno.h>
#include <unistd.h>

// Đọc/ghi đủ n byte (xử lý short read/write và EINTR)
ssize_t read_full(int fd, void *buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, (char *)buf + got, n - got);
        if (r == 0) return 0;              // peer đóng kết nối
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        got += r;
    }
    return got;
}

ssize_t write_full(int fd, const void *buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        ssize_t w = write(fd, (const char *)buf + sent, n - sent);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        sent += w;
    }
    return sent;
}

int send_msg(int fd, const char *data, uint32_t len) {
    uint32_t nlen = htonl(len);
    if (write_full(fd, &nlen, 4) < 0) return -1;
    return write_full(fd, data, len) < 0 ? -1 : 0;
}