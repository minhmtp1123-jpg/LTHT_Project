/*
 * server.c - Chat server da client dung epoll (non-blocking)
 *            Lang nghe dong thoi tren Unix domain socket va TCP.
 *
 * Giao thuc: moi tin nhan = [4 byte do dai, big-endian][payload]
 *            (length-prefix framing, payload toi da MAX_PAYLOAD byte)
 *
 * Build:  gcc -Wall -Wextra -O2 -g -o server server.c
 *         gcc -Wall -Wextra -g -fsanitize=address,undefined -o server server.c
 * Chay:   ./server [unix_path] [tcp_port]
 *         mac dinh: /tmp/chat.sock  va  9000
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_UNIX_PATH "/tmp/chat.sock"
#define DEFAULT_TCP_PORT  9000
#define MAX_EVENTS        64
#define MAX_PAYLOAD       4096          /* payload toi da cua 1 frame */
#define RBUF_SIZE         (4 + MAX_PAYLOAD)
#define WBUF_LIMIT        (1 << 20)     /* 1 MiB: vuot qua -> ngat client cham */
#define NICK_MAX          16

/* ------------------------------------------------------------------ */
/* Cau truc du lieu                                                    */
/* ------------------------------------------------------------------ */

enum kind { K_LISTEN_UNIX, K_LISTEN_TCP, K_CLIENT };

/* Moi thu dang ky trong epoll deu bat dau bang struct handle,
 * nen epoll_event.data.ptr tro toi handle va ep kieu nguoc ve client. */
struct handle {
    enum kind kind;
    int fd;
};

struct client {
    struct handle h;            /* PHAI la thanh vien dau tien */
    char nick[NICK_MAX + 1];
    int is_unix;
    int dead;                   /* da dong fd, cho giai phong cuoi vong lap */

    /* buffer doc: gom byte den khi du 1 frame */
    uint8_t rbuf[RBUF_SIZE];
    size_t rlen;

    /* buffer ghi: hang doi byte chua gui het (khi send() tra EAGAIN) */
    uint8_t *wbuf;
    size_t wlen, woff, wcap;
    int want_out;               /* dang dang ky EPOLLOUT */

    struct client *prev, *next; /* danh sach lien ket de broadcast */
};

static int g_ep = -1;
static struct client *g_clients = NULL;
static int g_nclients = 0;
static volatile sig_atomic_t g_stop = 0;

static struct handle g_lunix = { K_LISTEN_UNIX, -1 };
static struct handle g_ltcp  = { K_LISTEN_TCP,  -1 };
static const char *g_unix_path = DEFAULT_UNIX_PATH;

/* ------------------------------------------------------------------ */
/* Tien ich                                                            */
/* ------------------------------------------------------------------ */

static void log_msg(const char *fmt, ...)
{
    char ts[16];
    time_t t = time(NULL);
    struct tm tm;
    localtime_r(&t, &tm);
    strftime(ts, sizeof ts, "%H:%M:%S", &tm);

    fprintf(stderr, "[%s] ", ts);
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
}

static void die(const char *what)
{
    perror(what);
    exit(EXIT_FAILURE);
}

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

/* ------------------------------------------------------------------ */
/* Quan ly client                                                      */
/* ------------------------------------------------------------------ */

static void epoll_set_out(struct client *c, int want_out)
{
    struct epoll_event ev;
    memset(&ev, 0, sizeof ev);
    ev.events = EPOLLIN | EPOLLRDHUP | (want_out ? EPOLLOUT : 0);
    ev.data.ptr = &c->h;
    if (epoll_ctl(g_ep, EPOLL_CTL_MOD, c->h.fd, &ev) < 0)
        log_msg("epoll_ctl MOD fd=%d: %s", c->h.fd, strerror(errno));
    c->want_out = want_out;
}

static struct client *find_by_nick(const char *nick)
{
    for (struct client *c = g_clients; c; c = c->next)
        if (!c->dead && strcmp(c->nick, nick) == 0)
            return c;
    return NULL;
}

static int queue_frame(struct client *c, const char *data, size_t len);

/* Gui mot dong text toi mot client */
static void send_text(struct client *c, const char *fmt, ...)
{
    char buf[MAX_PAYLOAD];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    if (n < 0)
        return;
    if ((size_t)n >= sizeof buf)
        n = sizeof buf - 1;
    queue_frame(c, buf, (size_t)n);
}

/* Gui toi tat ca client (tru `except`, co the NULL) */
static void broadcast(const struct client *except, const char *fmt, ...)
{
    char buf[MAX_PAYLOAD];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    if (n < 0)
        return;
    if ((size_t)n >= sizeof buf)
        n = sizeof buf - 1;

    for (struct client *c = g_clients; c; c = c->next)
        if (!c->dead && c != except)
            queue_frame(c, buf, (size_t)n);
}

/*
 * Dong fd va danh dau chet. KHONG free o day, vi con co the dang duyet
 * danh sach (broadcast) hoac con con tro trong mang events cua epoll_wait.
 * Viec free thuc su lam o sweep_dead() cuoi moi vong lap.
 */
static void kill_client(struct client *c, const char *why)
{
    if (c->dead)
        return;
    c->dead = 1;
    epoll_ctl(g_ep, EPOLL_CTL_DEL, c->h.fd, NULL);
    close(c->h.fd);
    g_nclients--;
    log_msg("fd=%d (%s) roi di: %s [con %d client]",
            c->h.fd, c->nick, why, g_nclients);
    broadcast(c, "*** %s da roi phong", c->nick);
}

static void sweep_dead(void)
{
    struct client *c = g_clients;
    while (c) {
        struct client *next = c->next;
        if (c->dead) {
            if (c->prev) c->prev->next = c->next;
            else         g_clients = c->next;
            if (c->next) c->next->prev = c->prev;
            free(c->wbuf);
            free(c);
        }
        c = next;
    }
}

/* ------------------------------------------------------------------ */
/* Ghi: hang doi + flush khi socket san sang                           */
/* ------------------------------------------------------------------ */

/* Day het du lieu dang cho. Tra 0 neu ok, -1 neu client da bi kill. */
static int flush_client(struct client *c)
{
    while (c->woff < c->wlen) {
        ssize_t w = send(c->h.fd, c->wbuf + c->woff, c->wlen - c->woff,
                         MSG_NOSIGNAL);
        if (w < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (!c->want_out)
                    epoll_set_out(c, 1);   /* cho EPOLLOUT roi gui tiep */
                return 0;
            }
            kill_client(c, strerror(errno));   /* EPIPE, ECONNRESET, ... */
            return -1;
        }
        c->woff += (size_t)w;
    }
    /* gui het */
    c->wlen = c->woff = 0;
    if (c->want_out)
        epoll_set_out(c, 0);
    return 0;
}

/* Dong goi 1 frame [len][data] vao hang doi ghi cua client roi thu gui */
static int queue_frame(struct client *c, const char *data, size_t len)
{
    if (c->dead)
        return -1;

    size_t pending = c->wlen - c->woff;
    size_t need = 4 + len;

    if (pending + need > WBUF_LIMIT) {
        kill_client(c, "buffer ghi tran (client doc qua cham)");
        return -1;
    }

    if (c->wlen + need > c->wcap) {
        if (c->woff) {                         /* don cho trong dau buffer */
            memmove(c->wbuf, c->wbuf + c->woff, pending);
            c->wlen = pending;
            c->woff = 0;
        }
        if (c->wlen + need > c->wcap) {
            size_t ncap = c->wcap ? c->wcap : 512;
            while (ncap < c->wlen + need)
                ncap *= 2;
            uint8_t *nb = realloc(c->wbuf, ncap);
            if (!nb) {
                kill_client(c, "het bo nho");
                return -1;
            }
            c->wbuf = nb;
            c->wcap = ncap;
        }
    }

    uint32_t nlen = htonl((uint32_t)len);
    memcpy(c->wbuf + c->wlen, &nlen, 4);
    memcpy(c->wbuf + c->wlen + 4, data, len);
    c->wlen += need;

    /* Neu da co EPOLLOUT dang cho thi khong can thu gui ngay */
    if (c->want_out)
        return 0;
    return flush_client(c);
}

/* ------------------------------------------------------------------ */
/* Xu ly lenh / tin nhan                                               */
/* ------------------------------------------------------------------ */

static int valid_nick(const char *s)
{
    size_t n = strlen(s);
    if (n == 0 || n > NICK_MAX)
        return 0;
    for (size_t i = 0; i < n; i++)
        if (!isalnum((unsigned char)s[i]) && s[i] != '_')
            return 0;
    return 1;
}

static void handle_message(struct client *c, char *msg)
{
    /* cat khoang trang / xuong dong o cuoi */
    size_t n = strlen(msg);
    while (n > 0 && isspace((unsigned char)msg[n - 1]))
        msg[--n] = '\0';
    if (n == 0)
        return;

    if (msg[0] != '/') {                       /* tin thuong -> broadcast */
        log_msg("[%s] %s", c->nick, msg);
        broadcast(NULL, "[%s] %s", c->nick, msg);
        return;
    }

    if (strncmp(msg, "/nick ", 6) == 0) {
        char *nn = msg + 6;
        while (*nn == ' ') nn++;
        if (!valid_nick(nn)) {
            send_text(c, "!! nick khong hop le (A-Z a-z 0-9 _, toi da %d ky tu)",
                      NICK_MAX);
        } else if (find_by_nick(nn)) {
            send_text(c, "!! nick '%s' da co nguoi dung", nn);
        } else {
            char old[NICK_MAX + 1];
            snprintf(old, sizeof old, "%s", c->nick);
            snprintf(c->nick, sizeof c->nick, "%s", nn);
            broadcast(NULL, "*** %s doi ten thanh %s", old, c->nick);
        }
    } else if (strcmp(msg, "/list") == 0) {
        send_text(c, "*** %d nguoi dang online:", g_nclients);
        for (struct client *o = g_clients; o; o = o->next)
            if (!o->dead)
                send_text(c, "  - %s%s%s", o->nick, o->is_unix ? " (unix)" : " (tcp)",
                          o == c ? " <- ban" : "");
    } else if (strncmp(msg, "/msg ", 5) == 0) {
        char *to = msg + 5;
        while (*to == ' ') to++;
        char *text = strchr(to, ' ');
        if (!text) {
            send_text(c, "!! cu phap: /msg <nick> <noi dung>");
            return;
        }
        *text++ = '\0';
        struct client *dst = find_by_nick(to);
        if (!dst) {
            send_text(c, "!! khong tim thay '%s'", to);
            return;
        }
        send_text(dst, "[rieng tu tu %s] %s", c->nick, text);
        send_text(c, "[rieng tu toi %s] %s", dst->nick, text);
    } else if (strcmp(msg, "/help") == 0) {
        send_text(c, "*** /nick <ten> | /list | /msg <ten> <nd> | /quit | /help");
    } else if (strcmp(msg, "/quit") == 0) {
        send_text(c, "*** tam biet!");
        kill_client(c, "/quit");
    } else {
        send_text(c, "!! lenh khong ro, go /help");
    }
}

/* ------------------------------------------------------------------ */
/* Doc: gom byte -> tach frame                                         */
/* ------------------------------------------------------------------ */

static void on_readable(struct client *c)
{
    ssize_t n;
    do {
        n = read(c->h.fd, c->rbuf + c->rlen, RBUF_SIZE - c->rlen);
    } while (n < 0 && errno == EINTR);

    if (n == 0) {
        kill_client(c, "dong ket noi (EOF)");
        return;
    }
    if (n < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            kill_client(c, strerror(errno));
        return;
    }
    c->rlen += (size_t)n;

    /* Tach tat ca frame hoan chinh dang co trong buffer */
    size_t off = 0;
    while (c->rlen - off >= 4) {
        uint32_t len;
        memcpy(&len, c->rbuf + off, 4);
        len = ntohl(len);

        if (len == 0 || len > MAX_PAYLOAD) {
            kill_client(c, "frame khong hop le (sai do dai)");
            return;
        }
        if (c->rlen - off < 4 + (size_t)len)
            break;                              /* frame chua den du */

        char msg[MAX_PAYLOAD + 1];
        memcpy(msg, c->rbuf + off + 4, len);
        msg[len] = '\0';
        off += 4 + (size_t)len;

        handle_message(c, msg);
        if (c->dead)
            return;
    }

    /* Giu lai phan du (frame dang do) o dau buffer */
    if (off) {
        memmove(c->rbuf, c->rbuf + off, c->rlen - off);
        c->rlen -= off;
    }
}

/* ------------------------------------------------------------------ */
/* Accept                                                              */
/* ------------------------------------------------------------------ */

static void accept_clients(struct handle *lh)
{
    for (;;) {
        struct sockaddr_storage ss;
        socklen_t sl = sizeof ss;
        int fd = accept4(lh->fd, (struct sockaddr *)&ss, &sl,
                         SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            if (errno == EMFILE || errno == ENFILE) {
                log_msg("accept4: het file descriptor");
                return;
            }
            log_msg("accept4: %s", strerror(errno));
            return;
        }

        struct client *c = calloc(1, sizeof *c);
        if (!c) {
            close(fd);
            continue;
        }
        c->h.kind = K_CLIENT;
        c->h.fd = fd;
        c->is_unix = (lh->kind == K_LISTEN_UNIX);
        snprintf(c->nick, sizeof c->nick, "user%d", fd);

        char who[96] = "";
        if (c->is_unix) {
            struct ucred cred;
            socklen_t cl = sizeof cred;
            if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &cl) == 0)
                snprintf(who, sizeof who, "unix pid=%d uid=%u",
                         cred.pid, cred.uid);
            else
                snprintf(who, sizeof who, "unix");
        } else {
            char ip[INET6_ADDRSTRLEN] = "?";
            int one = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
            if (ss.ss_family == AF_INET) {
                struct sockaddr_in *a = (struct sockaddr_in *)&ss;
                inet_ntop(AF_INET, &a->sin_addr, ip, sizeof ip);
                snprintf(who, sizeof who, "tcp %s:%u", ip, ntohs(a->sin_port));
            } else {
                snprintf(who, sizeof who, "tcp");
            }
        }

        struct epoll_event ev;
        memset(&ev, 0, sizeof ev);
        ev.events = EPOLLIN | EPOLLRDHUP;
        ev.data.ptr = &c->h;
        if (epoll_ctl(g_ep, EPOLL_CTL_ADD, fd, &ev) < 0) {
            log_msg("epoll_ctl ADD: %s", strerror(errno));
            close(fd);
            free(c);
            continue;
        }

        /* them vao dau danh sach */
        c->next = g_clients;
        if (g_clients)
            g_clients->prev = c;
        g_clients = c;
        g_nclients++;

        log_msg("fd=%d ket noi (%s) -> %s [co %d client]",
                fd, who, c->nick, g_nclients);
        send_text(c, "*** Chao mung %s! Go /help de xem lenh.", c->nick);
        broadcast(c, "*** %s vua vao phong", c->nick);
    }
}

/* ------------------------------------------------------------------ */
/* Tao socket lang nghe                                                */
/* ------------------------------------------------------------------ */

static int make_unix_listener(const char *path)
{
    struct sockaddr_un addr;
    if (strlen(path) >= sizeof addr.sun_path) {
        fprintf(stderr, "duong dan unix socket qua dai (toi da %zu)\n",
                sizeof addr.sun_path - 1);
        exit(EXIT_FAILURE);
    }
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0)
        die("socket(AF_UNIX)");

    unlink(path);                               /* xoa file cu neu con sot */
    memset(&addr, 0, sizeof addr);
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path);
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0)
        die("bind(unix)");
    chmod(path, 0666);                          /* cho user khac ket noi duoc */
    if (listen(fd, SOMAXCONN) < 0)
        die("listen(unix)");
    return fd;
}

static int make_tcp_listener(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0)
        die("socket(AF_INET)");

    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one) < 0)
        die("setsockopt(SO_REUSEADDR)");

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0)
        die("bind(tcp)");
    if (listen(fd, SOMAXCONN) < 0)
        die("listen(tcp)");
    return fd;
}

static void add_listener(struct handle *lh)
{
    struct epoll_event ev;
    memset(&ev, 0, sizeof ev);
    ev.events = EPOLLIN;
    ev.data.ptr = lh;
    if (epoll_ctl(g_ep, EPOLL_CTL_ADD, lh->fd, &ev) < 0)
        die("epoll_ctl ADD listener");
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
    uint16_t port = DEFAULT_TCP_PORT;
    if (argc > 1)
        g_unix_path = argv[1];
    if (argc > 2) {
        long p = strtol(argv[2], NULL, 10);
        if (p <= 0 || p > 65535) {
            fprintf(stderr, "dung: %s [unix_path] [tcp_port]\n", argv[0]);
            return EXIT_FAILURE;
        }
        port = (uint16_t)p;
    }

    /* SIGPIPE: bo qua (da co MSG_NOSIGNAL, day la lop bao ve thu hai).
     * SIGINT/SIGTERM: dat co g_stop; KHONG dung SA_RESTART de epoll_wait
     * tra ve EINTR va vong lap thoat duoc. */
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    g_ep = epoll_create1(EPOLL_CLOEXEC);
    if (g_ep < 0)
        die("epoll_create1");

    g_lunix.fd = make_unix_listener(g_unix_path);
    g_ltcp.fd  = make_tcp_listener(port);
    add_listener(&g_lunix);
    add_listener(&g_ltcp);

    log_msg("server san sang: unix=%s tcp=:%u (Ctrl+C de dung)",
            g_unix_path, port);

    struct epoll_event evs[MAX_EVENTS];
    while (!g_stop) {
        int n = epoll_wait(g_ep, evs, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < n; i++) {
            struct handle *h = evs[i].data.ptr;

            if (h->kind == K_LISTEN_UNIX || h->kind == K_LISTEN_TCP) {
                accept_clients(h);
                continue;
            }

            struct client *c = (struct client *)h;
            if (c->dead)
                continue;

            uint32_t e = evs[i].events;
            if (e & EPOLLIN)
                on_readable(c);
            if (!c->dead && (e & EPOLLOUT))
                flush_client(c);
            if (!c->dead && (e & (EPOLLERR | EPOLLHUP)))
                kill_client(c, "loi / hang-up");
        }

        sweep_dead();   /* chi free sau khi het batch event */
    }

    /* Don dep */
    log_msg("dang tat server...");
    for (struct client *c = g_clients; c; c = c->next)
        if (!c->dead)
            close(c->h.fd);
    while (g_clients) {
        struct client *next = g_clients->next;
        free(g_clients->wbuf);
        free(g_clients);
        g_clients = next;
    }
    close(g_lunix.fd);
    close(g_ltcp.fd);
    close(g_ep);
    unlink(g_unix_path);
    return EXIT_SUCCESS;
}