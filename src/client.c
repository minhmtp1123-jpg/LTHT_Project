/*
 * client.c - Client cho chat server (Unix domain socket hoac TCP)
 *            Dang nhap bang username + mat khau, sau do dung poll() cho
 *            stdin va socket cung luc.
 *
 * Giao thuc: [4 byte do dai, big-endian][payload]  (giong server.c)
 *
 * Build:  gcc -Wall -Wextra -O2 -g -o client client.c
 * Chay:   ./client [-r] unix [duong_dan]     mac dinh /tmp/chat.sock
 *         ./client [-r] tcp  [host] [port]   mac dinh 127.0.0.1 9000
 *         -r : dang ky tai khoan moi (hoi mat khau 2 lan) thay vi dang nhap
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
#include <termios.h>
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

/* Ctrl+C luc dang go mat khau: tra lai echo cho terminal roi thoat */
static void on_sigint(int sig)
{
    restore_tty();
    _exit(128 + sig);
}

/* Doc 1 dong tu stdin bang read() tung byte (khong dung stdio de khong
 * "an" mat du lieu cua cac dong sau). Tra: do dai, -1 neu EOF, -2 neu qua dai. */
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

/* Hoi thong tin; secret != 0 thi tat echo neu stdin la terminal */
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

/* Tao chuoi lenh "/login <ten> <mat khau>" hoac "/register ...".
 * Tra 0 neu ok, -1 neu stdin het (EOF). */
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

static int read_exact(int fd, void *buf, size_t n)
{
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, (char *)buf + got, n - got);
        if (r == 0)
            return -1;
        if (r < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        got += (size_t)r;
    }
    return 0;
}

/* Doc dung 1 frame (blocking), tra do dai payload hoac -1 */
static int read_frame(int fd, char *buf, size_t cap)
{
    uint32_t len;
    if (read_exact(fd, &len, 4) < 0)
        return -1;
    len = ntohl(len);
    if (len == 0 || len >= cap)
        return -1;
    if (read_exact(fd, buf, len) < 0)
        return -1;
    buf[len] = '\0';
    return (int)len;
}

/* Cho toi 300 ms xem server da dong ket noi chua */
static int server_closed(int fd)
{
    struct pollfd p = { .fd = fd, .events = POLLIN };
    char c;
    if (poll(&p, 1, 300) > 0)
        return recv(fd, &c, 1, MSG_PEEK | MSG_DONTWAIT) == 0;
    return 0;
}

/* Gui /login hoac /register, cho "+OK" hoac "-ERR <ly do>".
 * Neu bi tu choi thi cho nhap lai (server ngat sau vai lan sai). */
static int authenticate(int fd, int reg)
{
    char reply[MAX_PAYLOAD + 1];

    for (;;) {
        char cmd[256];
        if (ask_credentials(reg, cmd, sizeof cmd) < 0)
            return -1;
        int s = send_msg(fd, cmd, strlen(cmd));
        explicit_bzero(cmd, sizeof cmd);
        if (s < 0) {
            fprintf(stderr, "[client] loi gui: %s\n", strerror(errno));
            return -1;
        }

        for (;;) {                       /* bo qua cac frame khong phai +OK/-ERR */
            if (read_frame(fd, reply, sizeof reply) < 0) {
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
    int ai = 1;                          /* chi so cua "unix" / "tcp" */

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