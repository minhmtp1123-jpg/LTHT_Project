/*
 * client.c - Client cho chat server (Unix domain socket hoac TCP)
 *            Dung poll() cho stdin va socket cung luc.
 *
 * Giao thuc: [4 byte do dai, big-endian][payload]  (giong server.c)
 *
 * Build:  gcc -Wall -Wextra -O2 -g -o client client.c
 * Chay:   ./client unix [duong_dan]          mac dinh /tmp/chat.sock
 *         ./client tcp  [host] [port]        mac dinh 127.0.0.1 9000
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#define MAX_PAYLOAD 4096
#define RBUF_SIZE   (4 + MAX_PAYLOAD)

static void die(const char *what)
{
    perror(what);
    exit(EXIT_FAILURE);
}

/* ---------- ket noi ---------- */

static int connect_unix(const char *path)
{
    struct sockaddr_un addr;
    if (strlen(path) >= sizeof addr.sun_path) {
        fprintf(stderr, "duong dan qua dai\n");
        exit(EXIT_FAILURE);
    }
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0)
        die("socket");
    memset(&addr, 0, sizeof addr);
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path);
    if (connect(fd, (struct sockaddr *)&addr, sizeof addr) < 0)
        die("connect(unix)");
    return fd;
}

static int connect_tcp(const char *host, const char *port)
{
    struct addrinfo hints, *res, *p;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    int rc = getaddrinfo(host, port, &hints, &res);
    if (rc != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rc));
        exit(EXIT_FAILURE);
    }
    int fd = -1;
    for (p = res; p; p = p->ai_next) {
        fd = socket(p->ai_family, p->ai_socktype | SOCK_CLOEXEC, p->ai_protocol);
        if (fd < 0)
            continue;
        if (connect(fd, p->ai_addr, p->ai_addrlen) == 0)
            break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) {
        fprintf(stderr, "khong ket noi duoc toi %s:%s\n", host, port);
        exit(EXIT_FAILURE);
    }
    return fd;
}

/* ---------- gui frame (blocking, xu ly short write) ---------- */

static int write_full(int fd, const void *buf, size_t n)
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

static int send_msg(int fd, const char *data, size_t len)
{
    if (len == 0 || len > MAX_PAYLOAD)
        return 0;
    uint8_t buf[RBUF_SIZE];
    uint32_t nlen = htonl((uint32_t)len);
    memcpy(buf, &nlen, 4);
    memcpy(buf + 4, data, len);          /* gop 1 lan send de tranh 2 goi nho */
    return write_full(fd, buf, 4 + len);
}

/* ---------- nhan frame: gom byte -> tach frame ---------- */

/* Tra 0 = ok, -1 = server dong / loi / frame sai */
static int on_socket_readable(int fd, uint8_t *rbuf, size_t *rlen)
{
    ssize_t n;
    do {
        n = read(fd, rbuf + *rlen, RBUF_SIZE - *rlen);
    } while (n < 0 && errno == EINTR);

    if (n == 0)
        return -1;
    if (n < 0)
        return (errno == EAGAIN) ? 0 : -1;
    *rlen += (size_t)n;

    size_t off = 0;
    while (*rlen - off >= 4) {
        uint32_t len;
        memcpy(&len, rbuf + off, 4);
        len = ntohl(len);
        if (len == 0 || len > MAX_PAYLOAD) {
            fprintf(stderr, "[client] frame sai tu server (len=%u)\n", len);
            return -1;
        }
        if (*rlen - off < 4 + (size_t)len)
            break;
        fwrite(rbuf + off + 4, 1, len, stdout);
        fputc('\n', stdout);
        fflush(stdout);
        off += 4 + (size_t)len;
    }
    if (off) {
        memmove(rbuf, rbuf + off, *rlen - off);
        *rlen -= off;
    }
    return 0;
}

/* ---------- doc stdin theo dong (khong dung stdio de tron voi poll) ---------- */

/* Tra 0 = ok, 1 = stdin EOF, -1 = loi gui */
static int on_stdin_readable(int sock, char *lbuf, size_t *llen)
{
    ssize_t n;
    do {
        n = read(STDIN_FILENO, lbuf + *llen, MAX_PAYLOAD - *llen);
    } while (n < 0 && errno == EINTR);

    if (n < 0)
        return (errno == EAGAIN) ? 0 : -1;
    if (n == 0) {                        /* EOF: gui not phan con lai (neu co) */
        if (*llen > 0 && send_msg(sock, lbuf, *llen) < 0)
            return -1;
        *llen = 0;
        return 1;
    }
    *llen += (size_t)n;

    size_t start = 0;
    for (size_t i = 0; i < *llen; i++) {
        if (lbuf[i] == '\n') {
            size_t len = i - start;
            if (len > 0 && lbuf[start + len - 1] == '\r')
                len--;
            if (send_msg(sock, lbuf + start, len) < 0)
                return -1;
            start = i + 1;
        }
    }
    if (start) {
        memmove(lbuf, lbuf + start, *llen - start);
        *llen -= start;
    }
    if (*llen == MAX_PAYLOAD) {          /* dong qua dai: gui luon, khong cho \n */
        if (send_msg(sock, lbuf, *llen) < 0)
            return -1;
        *llen = 0;
    }
    return 0;
}

int main(int argc, char **argv)
{
    int fd;
    if (argc >= 2 && strcmp(argv[1], "unix") == 0) {
        fd = connect_unix(argc > 2 ? argv[2] : "/tmp/chat.sock");
    } else if (argc >= 2 && strcmp(argv[1], "tcp") == 0) {
        fd = connect_tcp(argc > 2 ? argv[2] : "127.0.0.1",
                         argc > 3 ? argv[3] : "9000");
    } else {
        fprintf(stderr, "dung: %s unix [path] | %s tcp [host] [port]\n",
                argv[0], argv[0]);
        return EXIT_FAILURE;
    }

    signal(SIGPIPE, SIG_IGN);

    uint8_t rbuf[RBUF_SIZE];
    size_t rlen = 0;
    char lbuf[MAX_PAYLOAD];
    size_t llen = 0;

    struct pollfd pfd[2] = {
        { .fd = STDIN_FILENO, .events = POLLIN },
        { .fd = fd,           .events = POLLIN },
    };

    for (;;) {
        if (poll(pfd, 2, -1) < 0) {
            if (errno == EINTR)
                continue;
            die("poll");
        }

        /* Uu tien doc socket truoc de khong mat dong cuoi khi server dong */
        if (pfd[1].revents & (POLLIN | POLLHUP | POLLERR)) {
            if (on_socket_readable(fd, rbuf, &rlen) < 0) {
                fprintf(stderr, "[client] server da dong ket noi\n");
                break;
            }
        }

        if (pfd[0].fd >= 0 && (pfd[0].revents & (POLLIN | POLLHUP))) {
            int r = on_stdin_readable(fd, lbuf, &llen);
            if (r < 0) {
                fprintf(stderr, "[client] loi gui: %s\n", strerror(errno));
                break;
            }
            if (r == 1) {                /* stdin het: bao server, cho no dong */
                shutdown(fd, SHUT_WR);
                pfd[0].fd = -1;          /* poll bo qua fd am */
            }
        }
    }

    close(fd);
    return EXIT_SUCCESS;
}