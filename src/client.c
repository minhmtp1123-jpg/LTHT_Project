/*
 * client.c - Client cho chat server (Unix domain socket hoac TCP)
 *            Dang nhap bang username + mat khau, sau do dung poll() cho
 *            stdin va socket cung luc.
 *            Ho tro ANSI line management chong de dong chat khi dang go.
 *
 * Giao thuc: [4 byte do dai, big-endian][payload] (dung chung qua proto.h)
 *
 * Build:  gcc -Wall -Wextra -O2 -g -o client client.c proto.c
 * Chay:   ./client [-r] unix [duong_dan]     mac dinh /tmp/chat.sock
 *         ./client [-r] tcp  [host] [port]   mac dinh 127.0.0.1 9000
 *         -r : dang ky tai khoan moi (hoi mat khau 2 lan) thay vi dang nhap
 */
#define _GNU_SOURCE
#include "proto.h"

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
#include <termios.h>
#include <unistd.h>

static int g_is_tty = 0;
static char g_input_buf[MAX_PAYLOAD];
static size_t g_input_len = 0;

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
    while (*rlen - off >= HEADER_SIZE) {
        uint32_t len;
        memcpy(&len, rbuf + off, HEADER_SIZE);
        len = ntohl(len);
        if (len == 0 || len > MAX_PAYLOAD) {
            fprintf(stderr, "[client] frame sai tu server (len=%u)\n", len);
            return -1;
        }
        if (*rlen - off < HEADER_SIZE + (size_t)len)
            break;

        /* In thong diep, ho tro ANSI line management de khong de len chu dang go */
        if (g_is_tty) {
            fputs("\r\033[K", stdout);
            fwrite(rbuf + off + HEADER_SIZE, 1, len, stdout);
            fputc('\n', stdout);
            fputs("> ", stdout);
            if (g_input_len > 0)
                fwrite(g_input_buf, 1, g_input_len, stdout);
            fflush(stdout);
        } else {
            fwrite(rbuf + off + HEADER_SIZE, 1, len, stdout);
            fputc('\n', stdout);
            fflush(stdout);
        }

        off += HEADER_SIZE + (size_t)len;
    }
    if (off) {
        memmove(rbuf, rbuf + off, *rlen - off);
        *rlen -= off;
    }
    return 0;
}

/* ---------- doc stdin theo dong (khong dung stdio de tron voi poll) ---------- */

/* Tra 0 = ok, 1 = stdin EOF, -1 = loi gui */
static int on_stdin_readable(int sock)
{
    ssize_t n;
    do {
        n = read(STDIN_FILENO, g_input_buf + g_input_len, MAX_PAYLOAD - g_input_len);
    } while (n < 0 && errno == EINTR);

    if (n < 0)
        return (errno == EAGAIN) ? 0 : -1;
    if (n == 0) {
        if (g_input_len > 0 && send_frame_blocking(sock, g_input_buf, g_input_len) < 0)
            return -1;
        g_input_len = 0;
        return 1;
    }
    g_input_len += (size_t)n;

    size_t start = 0;
    for (size_t i = 0; i < g_input_len; i++) {
        if (g_input_buf[i] == '\n') {
            size_t len = i - start;
            if (len > 0 && g_input_buf[start + len - 1] == '\r')
                len--;
            if (send_frame_blocking(sock, g_input_buf + start, len) < 0)
                return -1;
            start = i + 1;
            if (g_is_tty) {
                fputs("> ", stdout);
                fflush(stdout);
            }
        }
    }
    if (start) {
        memmove(g_input_buf, g_input_buf + start, g_input_len - start);
        g_input_len -= start;
    }
    if (g_input_len == MAX_PAYLOAD) {
        if (send_frame_blocking(sock, g_input_buf, g_input_len) < 0)
            return -1;
        g_input_len = 0;
        if (g_is_tty) {
            fputs("> ", stdout);
            fflush(stdout);
        }
    }
    return 0;
}

/* ---------- dang nhap / dang ky ---------- */

static struct termios g_old_tio;
static int g_tio_saved = 0;

static void restore_tty(void)
{
    if (g_tio_saved) {
        tcsetattr(STDIN_FILENO, TCSAFLUSH, &g_old_tio);
        g_tio_saved = 0;
    }
}

static void on_sigint(int sig)
{
    restore_tty();
    _exit(128 + sig);
}

static int read_line(char *buf, size_t cap)
{
    size_t n = 0;
    int too_long = 0;
    char ch;

    for (;;) {
        ssize_t r = read(STDIN_FILENO, &ch, 1);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (r == 0) {
            if (n == 0 && !too_long)
                return -1;
            break;
        }
        if (ch == '\n')
            break;
        if (n + 1 < cap)
            buf[n++] = ch;
        else
            too_long = 1;
    }
    buf[n] = '\0';
    if (n > 0 && buf[n - 1] == '\r')
        buf[--n] = '\0';
    return too_long ? -2 : (int)n;
}

static int prompt_line(const char *label, char *buf, size_t cap, int secret)
{
    fprintf(stderr, "%s", label);
    fflush(stderr);

    int hide = secret && isatty(STDIN_FILENO) && tcgetattr(STDIN_FILENO, &g_old_tio) == 0;
    struct sigaction sa, old_int, old_term;
    if (hide) {
        struct termios t = g_old_tio;
        t.c_lflag &= ~(tcflag_t)ECHO;
        memset(&sa, 0, sizeof sa);
        sa.sa_handler = on_sigint;
        sigemptyset(&sa.sa_mask);
        sigaction(SIGINT, &sa, &old_int);
        sigaction(SIGTERM, &sa, &old_term);
        g_tio_saved = 1;
        tcsetattr(STDIN_FILENO, TCSAFLUSH, &t);
    }

    int r = read_line(buf, cap);

    if (hide) {
        restore_tty();
        sigaction(SIGINT, &old_int, NULL);
        sigaction(SIGTERM, &old_term, NULL);
        fputc('\n', stderr);
    }
    return r;
}

static int ask_credentials(int reg, char *out, size_t outsz)
{
    char user[64], pass[128], pass2[128];

    for (;;) {
        int r = prompt_line("Ten dang nhap: ", user, sizeof user, 0);
        if (r == -1)
            return -1;
        if (r < 1 || strchr(user, ' ')) {
            fprintf(stderr, "Ten dang nhap khong duoc rong hoac chua dau cach.\n");
            continue;
        }

        r = prompt_line("Mat khau: ", pass, sizeof pass, 1);
        if (r == -1)
            return -1;
        if (r < 1) {
            fprintf(stderr, "Mat khau khong duoc rong (toi da %zu ky tu).\n",
                    sizeof pass - 1);
            continue;
        }

        if (reg) {
            r = prompt_line("Nhap lai mat khau: ", pass2, sizeof pass2, 1);
            if (r == -1)
                return -1;
            if (strcmp(pass, pass2) != 0) {
                fprintf(stderr, "Hai mat khau khong khop, thu lai.\n");
                continue;
            }
        }
        break;
    }

    snprintf(out, outsz, "/%s %s %s", reg ? "register" : "login", user, pass);
    explicit_bzero(user, sizeof user);
    explicit_bzero(pass, sizeof pass);
    explicit_bzero(pass2, sizeof pass2);
    return 0;
}

static int server_closed(int fd)
{
    struct pollfd p = { .fd = fd, .events = POLLIN };
    char c;
    if (poll(&p, 1, 300) > 0)
        return recv(fd, &c, 1, MSG_PEEK | MSG_DONTWAIT) == 0;
    return 0;
}

static int authenticate(int fd, int reg)
{
    char reply[MAX_PAYLOAD + 1];

    for (;;) {
        char cmd[256];
        if (ask_credentials(reg, cmd, sizeof cmd) < 0)
            return -1;
        int s = send_frame_blocking(fd, cmd, strlen(cmd));
        explicit_bzero(cmd, sizeof cmd);
        if (s < 0) {
            fprintf(stderr, "[client] loi gui: %s\n", strerror(errno));
            return -1;
        }

        for (;;) {
            if (read_frame_blocking(fd, reply, sizeof reply) < 0) {
                fprintf(stderr, "[client] server da dong ket noi\n");
                return -1;
            }
            if (strncmp(reply, "+OK", 3) == 0) {
                fprintf(stderr, "%s thanh cong.\n", reg ? "Dang ky" : "Dang nhap");
                return 0;
            }
            if (strncmp(reply, "-ERR", 4) == 0) {
                const char *why = reply + 4;
                while (*why == ' ')
                    why++;
                fprintf(stderr, "Loi: %s\n", why);
                break;
            }
        }
        if (server_closed(fd)) {
            fprintf(stderr, "[client] server da ngat ket noi.\n");
            return -1;
        }
    }
}

int main(int argc, char **argv)
{
    int fd;
    int reg = 0;
    int ai = 1;

    if (argc > 1 && strcmp(argv[1], "-r") == 0) {
        reg = 1;
        ai = 2;
    }
    if (argc > ai && strcmp(argv[ai], "unix") == 0) {
        fd = connect_unix(argc > ai + 1 ? argv[ai + 1] : "/tmp/chat.sock");
    } else if (argc > ai && strcmp(argv[ai], "tcp") == 0) {
        fd = connect_tcp(argc > ai + 1 ? argv[ai + 1] : "127.0.0.1",
                         argc > ai + 2 ? argv[ai + 2] : "9000");
    } else {
        fprintf(stderr, "dung: %s [-r] unix [path] | %s [-r] tcp [host] [port]\n",
                argv[0], argv[0]);
        return EXIT_FAILURE;
    }

    signal(SIGPIPE, SIG_IGN);

    if (authenticate(fd, reg) < 0) {
        close(fd);
        return EXIT_FAILURE;
    }

    g_is_tty = isatty(STDIN_FILENO) && isatty(STDOUT_FILENO);
    if (g_is_tty) {
        fputs("> ", stdout);
        fflush(stdout);
    }

    uint8_t rbuf[RBUF_SIZE];
    size_t rlen = 0;

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

        if (pfd[1].revents & (POLLIN | POLLHUP | POLLERR)) {
            if (on_socket_readable(fd, rbuf, &rlen) < 0) {
                if (g_is_tty)
                    fputs("\r\033[K", stdout);
                fprintf(stderr, "[client] server da dong ket noi\n");
                break;
            }
        }

        if (pfd[0].fd >= 0 && (pfd[0].revents & (POLLIN | POLLHUP))) {
            int r = on_stdin_readable(fd);
            if (r < 0) {
                if (g_is_tty)
                    fputs("\r\033[K", stdout);
                fprintf(stderr, "[client] loi gui: %s\n", strerror(errno));
                break;
            }
            if (r == 1) {
                shutdown(fd, SHUT_WR);
                pfd[0].fd = -1;
            }
        }
    }

    close(fd);
    return EXIT_SUCCESS;
}
