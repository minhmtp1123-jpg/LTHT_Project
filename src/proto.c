/*
 * proto.c - Cai dat cac ham thao tac tren frame va I/O giao thuc chung.
 */
#define _GNU_SOURCE
#include "proto.h"

#include <arpa/inet.h>
#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

int write_full(int fd, const void *buf, size_t n)
{
    size_t sent = 0;
    while (sent < n) {
        ssize_t w = send(fd, (const char *)buf + sent, n - sent, MSG_NOSIGNAL);
        if (w < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        sent += (size_t)w;
    }
    return 0;
}

int read_exact(int fd, void *buf, size_t n)
{
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, (char *)buf + got, n - got);
        if (r == 0)
            return -1; /* EOF truoc khi du n byte */
        if (r < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        got += (size_t)r;
    }
    return 0;
}

ssize_t pack_frame(uint8_t *dst, size_t dst_cap, const void *data, size_t len)
{
    if (len == 0 || len > MAX_PAYLOAD)
        return -1;
    if (dst_cap < HEADER_SIZE + len)
        return -1;

    uint32_t nlen = htonl((uint32_t)len);
    memcpy(dst, &nlen, HEADER_SIZE);
    memcpy(dst + HEADER_SIZE, data, len);
    return (ssize_t)(HEADER_SIZE + len);
}

int send_frame_blocking(int fd, const void *data, size_t len)
{
    if (len == 0 || len > MAX_PAYLOAD)
        return 0;

    uint8_t buf[RBUF_SIZE];
    ssize_t total = pack_frame(buf, sizeof buf, data, len);
    if (total < 0)
        return -1;

    return write_full(fd, buf, (size_t)total);
}

int read_frame_blocking(int fd, char *out_buf, size_t cap)
{
    uint32_t nlen;
    if (read_exact(fd, &nlen, HEADER_SIZE) < 0)
        return -1;

    uint32_t len = ntohl(nlen);
    if (len == 0 || len > MAX_PAYLOAD || len >= cap)
        return -1;

    if (read_exact(fd, out_buf, len) < 0)
        return -1;

    out_buf[len] = '\0';
    return (int)len;
}
