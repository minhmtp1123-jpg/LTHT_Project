/*
 * server.c - Chat server da client dung epoll (non-blocking)
 *            Lang nghe dong thoi tren Unix domain socket va TCP (dual-stack IPv4/IPv6).
 *            Client phai dang nhap (username + mat khau) moi vao duoc phong.
 *
 * Giao thuc: moi tin nhan = [4 byte do dai, big-endian][payload]
 *            (length-prefix framing, payload toi da MAX_PAYLOAD byte tu proto.h)
 *
 * Xac thuc (truoc khi dang nhap chi nhan 2 lenh):
 *   /login <ten> <mat khau>      -> "+OK <ten>"  hoac "-ERR <ly do>"
 *   /register <ten> <mat khau>   -> "+OK <ten>"  hoac "-ERR <ly do>"
 *
 * Lenh sau khi dang nhap:
 *   /list                        -> xem ai dang online
 *   /msg <ten> <nd>              -> tin nhan rieng tu
 *   /whoami                      -> thong tin phien cua ban
 *   /stats                       -> thong ke trang thai may chu
 *   /passwd <cu> <moi>           -> doi mat khau tai khoan
 *   /quit                        -> roi phong
 *   /help                        -> huong dan
 *
 * Tep du lieu (tao tu dong neu chua co, quyen 0600):
 *   users.db  moi dong "ten:hash"  (hash tao bang crypt(3), mac dinh yescrypt)
 *   chat.log  moi dong "YYYY-MM-DD HH:MM:SS<TAB>nguoi_gui<TAB>noi_dung"
 *             nguoi_gui la "*" voi su kien he thong (vao/roi phong).
 *             Tin nhan rieng (/msg) KHONG duoc ghi vao lich su.
 */
#define _GNU_SOURCE
#include "proto.h"

#include <arpa/inet.h>
#include <crypt.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_UNIX_PATH  "/tmp/chat.sock"
#define DEFAULT_TCP_PORT   9000
#define DEFAULT_USERS_PATH "users.db"
#define DEFAULT_HIST_PATH  "chat.log"
#define MAX_EVENTS         64

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
    char nick[NICK_MAX + 1];    /* "user<fd>" truoc khi dang nhap, sau do = username */
    char peer_info[96];         /* Thong tin dia chi/dinh danh doi phuong */
    int is_unix;
    int dead;                   /* da dong fd, cho giai phong cuoi vong lap */
    int need_leave_notify;      /* co can thong bao roi phong o sweep_dead hay khong */

    int authed;                 /* da dang nhap thanh cong chua */
    int auth_fails;             /* so lan xac thuc that bai */
    time_t connected_at;

    /* buffer doc: gom byte den khi du 1 frame */
    uint8_t rbuf[RBUF_SIZE];
    size_t rlen;

    /* buffer ghi: hang doi byte chua gui het (khi send() tra EAGAIN) */
    uint8_t *wbuf;
    size_t wlen, woff, wcap;
    int want_out;               /* dang dang ky EPOLLOUT */

    struct client *prev, *next; /* danh sach lien ket de broadcast */
};

struct account {
    char name[NICK_MAX + 1];
    char *hash;                 /* chuoi crypt(3) day du, vd "$y$j9T$..." */
};

static int g_ep = -1;
static struct client *g_clients = NULL;
static int g_nclients = 0;
static volatile sig_atomic_t g_stop = 0;

static struct handle g_lunix = { K_LISTEN_UNIX, -1 };
static struct handle g_ltcp  = { K_LISTEN_TCP,  -1 };
static const char *g_unix_path  = DEFAULT_UNIX_PATH;
static const char *g_users_path = DEFAULT_USERS_PATH;
static const char *g_hist_path  = DEFAULT_HIST_PATH;

static struct account *g_acc = NULL;    /* bang tai khoan nap tu users.db */
static size_t g_nacc = 0, g_cacc = 0;
static int g_users_fd = -1;             /* mo O_APPEND de them tai khoan moi */
static int g_hist_fd = -1;              /* mo O_APPEND de ghi lich su */
static char g_dummy_hash[256];          /* bam gia: lam cham dong deu khi user khong ton tai */
static struct crypt_data g_cd;          /* bo nho cho crypt_r (server don luong) */

static time_t g_start_time = 0;         /* thoi gian bat dau chay server */
static unsigned long long g_total_messages = 0; /* tong so tin nhan cong khai da phat */

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

static int valid_name(const char *s)
{
    size_t n = strlen(s);
    if (n == 0 || n > NICK_MAX)
        return 0;
    for (size_t i = 0; i < n; i++)
        if (!isalnum((unsigned char)s[i]) && s[i] != '_')
            return 0;
    return 1;
}

/* ------------------------------------------------------------------ */
/* Mat khau: bam bang crypt(3) cua libxcrypt (yescrypt, du phong sha512) */
/* ------------------------------------------------------------------ */

static int make_hash(const char *pass, char *out, size_t outsz)
{
    static const char *const prefixes[] = { "$y$", "$6$" };
    uint8_t rnd[32];

    if (getrandom(rnd, sizeof rnd, 0) != (ssize_t)sizeof rnd)
        return -1;

    for (size_t i = 0; i < sizeof prefixes / sizeof prefixes[0]; i++) {
        char setting[CRYPT_GENSALT_OUTPUT_SIZE];
        if (!crypt_gensalt_rn(prefixes[i], 0, (const char *)rnd,
                              (int)sizeof rnd, setting, (int)sizeof setting))
            continue;
        char *h = crypt_r(pass, setting, &g_cd);
        if (h && h[0] != '*' && strlen(h) < outsz) {
            strcpy(out, h);
            explicit_bzero(&g_cd, sizeof g_cd);
            explicit_bzero(rnd, sizeof rnd);
            return 0;
        }
    }
    explicit_bzero(&g_cd, sizeof g_cd);
    explicit_bzero(rnd, sizeof rnd);
    return -1;
}

/* So sanh khong ro ri thoi gian. Tra 1 neu mat khau dung. */
static int verify_pass(const char *pass, const char *hash)
{
    int ok = 0;
    char *r = crypt_r(pass, hash, &g_cd);
    if (r && r[0] != '*') {
        size_t a = strlen(r), b = strlen(hash);
        if (a == b) {
            unsigned char diff = 0;
            for (size_t i = 0; i < a; i++)
                diff |= (unsigned char)(r[i] ^ hash[i]);
            ok = (diff == 0);
        }
    }
    explicit_bzero(&g_cd, sizeof g_cd);
    return ok;
}

/* ------------------------------------------------------------------ */
/* Tai khoan: nap users.db, them tai khoan moi, cap nhat mat khau       */
/* ------------------------------------------------------------------ */

static struct account *acc_find(const char *name)
{
    for (size_t i = 0; i < g_nacc; i++)
        if (strcmp(g_acc[i].name, name) == 0)
            return &g_acc[i];
    return NULL;
}

static int acc_push(const char *name, const char *hash)
{
    if (g_nacc == g_cacc) {
        size_t nc = g_cacc ? g_cacc * 2 : 16;
        struct account *na = realloc(g_acc, nc * sizeof *na);
        if (!na)
            return -1;
        g_acc = na;
        g_cacc = nc;
    }
    char *h = strdup(hash);
    if (!h)
        return -1;
    snprintf(g_acc[g_nacc].name, sizeof g_acc[g_nacc].name, "%s", name);
    g_acc[g_nacc].hash = h;
    g_nacc++;
    return 0;
}

/* Tep chua hash mat khau / lich su: chi cho chu so huu doc, ghi.
 * Neu tep da ton tai voi quyen long hon thi canh bao va siet lai. */
static void tighten_perms(int fd, const char *path)
{
    struct stat st;
    if (fstat(fd, &st) == 0 && (st.st_mode & 077)) {
        log_msg("canh bao: %s co quyen %04o, dat lai 0600", path,
                (unsigned)(st.st_mode & 07777));
        if (fchmod(fd, 0600) < 0)
            log_msg("khong doi duoc quyen %s: %s", path, strerror(errno));
    }
}

static void load_users(void)
{
    g_users_fd = open(g_users_path, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0600);
    if (g_users_fd < 0)
        die(g_users_path);
    tighten_perms(g_users_fd, g_users_path);

    FILE *f = fopen(g_users_path, "r");
    if (!f)
        die(g_users_path);

    char line[512];
    int lineno = 0;
    while (fgets(line, sizeof line, f)) {
        lineno++;
        size_t n = strlen(line);
        if (n > 0 && line[n - 1] == '\n') {
            line[--n] = '\0';
        } else if (n == sizeof line - 1) {        /* dong qua dai: bo qua */
            int ch;
            while ((ch = fgetc(f)) != '\n' && ch != EOF)
                ;
            log_msg("%s:%d dong qua dai, bo qua", g_users_path, lineno);
            continue;
        }
        if (n > 0 && line[n - 1] == '\r')
            line[--n] = '\0';
        if (n == 0 || line[0] == '#')
            continue;

        char *colon = strchr(line, ':');
        if (!colon || colon == line || colon[1] == '\0') {
            log_msg("%s:%d sai dinh dang, bo qua", g_users_path, lineno);
            continue;
        }
        *colon = '\0';
        if (!valid_name(line) || acc_find(line)) {
            log_msg("%s:%d ten khong hop le hoac bi trung, bo qua",
                    g_users_path, lineno);
            continue;
        }
        if (acc_push(line, colon + 1) < 0)
            die("malloc");
    }
    fclose(f);
    log_msg("da nap %zu tai khoan tu %s", g_nacc, g_users_path);
}

/* Bam mat khau, ghi 1 dong vao users.db (fsync), roi them vao bang trong RAM */
static int add_user(const char *name, const char *pass)
{
    char hash[256];
    if (make_hash(pass, hash, sizeof hash) < 0)
        return -1;

    char line[NICK_MAX + 1 + sizeof hash + 2];
    int n = snprintf(line, sizeof line, "%s:%s\n", name, hash);
    if (n < 0 || (size_t)n >= sizeof line)
        return -1;
    if (write(g_users_fd, line, (size_t)n) != n)
        return -1;
    fsync(g_users_fd);
    return acc_push(name, hash);
}

/* Cap nhat mat khau an toan bang atomic write qua file tam */
static int update_user_password(const char *name, const char *new_pass)
{
    struct account *a = acc_find(name);
    if (!a)
        return -1;

    char new_hash[256];
    if (make_hash(new_pass, new_hash, sizeof new_hash) < 0)
        return -1;

    char *nh = strdup(new_hash);
    if (!nh)
        return -1;

    char tmp_path[512];
    snprintf(tmp_path, sizeof tmp_path, "%s.tmp", g_users_path);
    FILE *tf = fopen(tmp_path, "w");
    if (!tf) {
        free(nh);
        return -1;
    }
    fchmod(fileno(tf), 0600);

    for (size_t i = 0; i < g_nacc; i++) {
        const char *h = (strcmp(g_acc[i].name, name) == 0) ? new_hash : g_acc[i].hash;
        if (fprintf(tf, "%s:%s\n", g_acc[i].name, h) < 0) {
            fclose(tf);
            unlink(tmp_path);
            free(nh);
            return -1;
        }
    }
    fflush(tf);
    fsync(fileno(tf));
    fclose(tf);

    if (rename(tmp_path, g_users_path) < 0) {
        unlink(tmp_path);
        free(nh);
        return -1;
    }

    if (g_users_fd >= 0)
        close(g_users_fd);
    g_users_fd = open(g_users_path, O_WRONLY | O_APPEND | O_CLOEXEC, 0600);

    free(a->hash);
    a->hash = nh;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Lich su chat: chat.log                                              */
/* ------------------------------------------------------------------ */

static void sanitize(char *s)
{
    for (; *s; s++)
        if ((unsigned char)*s < 0x20 || *s == 0x7f)
            *s = ' ';
}

static void hist_log(const char *who, const char *fmt, ...)
{
    if (g_hist_fd < 0)
        return;

    char text[MAX_PAYLOAD];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(text, sizeof text, fmt, ap);
    va_end(ap);
    sanitize(text);

    char ts[32];
    time_t t = time(NULL);
    struct tm tm;
    localtime_r(&t, &tm);
    strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", &tm);

    char line[MAX_PAYLOAD + 64];
    int n = snprintf(line, sizeof line, "%s\t%s\t%s\n", ts, who, text);
    if (n < 0)
        return;
    if ((size_t)n >= sizeof line) {
        n = (int)sizeof line - 1;
        line[n - 1] = '\n';
    }
    if (write(g_hist_fd, line, (size_t)n) != n)
        log_msg("ghi lich su loi: %s", strerror(errno));
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
        if (!c->dead && c->authed && strcmp(c->nick, nick) == 0)
            return c;
    return NULL;
}

static int count_authed(void)
{
    int n = 0;
    for (struct client *c = g_clients; c; c = c->next)
        if (!c->dead && c->authed)
            n++;
    return n;
}

static int queue_frame(struct client *c, const char *data, size_t len);

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
        if (!c->dead && c->authed && c != except)
            queue_frame(c, buf, (size_t)n);
}

/*
 * Dong fd va danh dau chet. KHONG goi broadcast o day de tranh de quy
 * (reentrancy) khi flush_client bi loi trong luc dang chay broadcast.
 * Viec broadcast "roi phong" va free thuc su duoc thuc hien o sweep_dead().
 */
static void kill_client(struct client *c, const char *why)
{
    if (c->dead)
        return;
    c->dead = 1;
    c->need_leave_notify = c->authed;
    epoll_ctl(g_ep, EPOLL_CTL_DEL, c->h.fd, NULL);
    close(c->h.fd);
    g_nclients--;
    log_msg("fd=%d (%s) roi di: %s [con %d ket noi]",
            c->h.fd, c->nick, why, g_nclients);
    if (c->authed)
        hist_log("*", "%s roi phong", c->nick);
}

static void sweep_dead(void)
{
    /* Pha 1: Thong bao cac client roi phong mot cach an toan (khong de quy) */
    for (struct client *c = g_clients; c; c = c->next) {
        if (c->dead && c->need_leave_notify) {
            c->need_leave_notify = 0;
            broadcast(c, "*** %s da roi phong", c->nick);
        }
    }

    /* Pha 2: Giai phong thuc su sau khi da duyet xong */
    struct client *c = g_clients;
    while (c) {
        struct client *next = c->next;
        if (c->dead) {
            if (c->prev) c->prev->next = c->next;
            else         g_clients = c->next;
            if (c->next) c->next->prev = c->prev;
            explicit_bzero(c->rbuf, sizeof c->rbuf);
            free(c->wbuf);
            free(c);
        }
        c = next;
    }
}

static void reap_unauthed(void)
{
    time_t now = time(NULL);
    for (struct client *c = g_clients; c; c = c->next)
        if (!c->dead && !c->authed && now - c->connected_at > AUTH_TIMEOUT_SEC) {
            send_text(c, "-ERR Het thoi gian dang nhap");
            kill_client(c, "het thoi gian xac thuc");
        }
}

/* ------------------------------------------------------------------ */
/* Ghi: hang doi + flush khi socket san sang                           */
/* ------------------------------------------------------------------ */

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
                    epoll_set_out(c, 1);
                return 0;
            }
            kill_client(c, strerror(errno));
            return -1;
        }
        c->woff += (size_t)w;
    }
    c->wlen = c->woff = 0;
    if (c->want_out)
        epoll_set_out(c, 0);
    return 0;
}

static int queue_frame(struct client *c, const char *data, size_t len)
{
    if (c->dead)
        return -1;

    size_t pending = c->wlen - c->woff;
    size_t need = HEADER_SIZE + len;

    if (pending + need > WBUF_LIMIT) {
        kill_client(c, "buffer ghi tran (client doc qua cham)");
        return -1;
    }

    if (c->wlen + need > c->wcap) {
        if (c->woff) {
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

    pack_frame(c->wbuf + c->wlen, c->wcap - c->wlen, data, len);
    c->wlen += need;

    if (c->want_out)
        return 0;
    return flush_client(c);
}

/* ------------------------------------------------------------------ */
/* Gui lai lich su gan day cho client vua dang nhap                    */
/* ------------------------------------------------------------------ */

static void send_history(struct client *c)
{
    static char buf[HISTORY_TAIL_BYTES + 1];

    int fd = open(g_hist_path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return;
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return;
    }
    off_t off = st.st_size > HISTORY_TAIL_BYTES ? st.st_size - HISTORY_TAIL_BYTES : 0;
    ssize_t n = pread(fd, buf, HISTORY_TAIL_BYTES, off);
    close(fd);
    if (n <= 0)
        return;
    buf[n] = '\0';

    char *p = buf;
    if (off > 0) {
        char *nl = memchr(buf, '\n', (size_t)n);
        if (!nl)
            return;
        p = nl + 1;
    }

    char *ring[HISTORY_REPLAY];
    size_t count = 0;
    while (*p) {
        char *nl = strchr(p, '\n');
        if (!nl)
            break;
        *nl = '\0';
        ring[count % HISTORY_REPLAY] = p;
        count++;
        p = nl + 1;
    }
    if (count == 0)
        return;

    send_text(c, "*** --- lich su gan day ---");
    size_t first = count > HISTORY_REPLAY ? count - HISTORY_REPLAY : 0;
    for (size_t i = first; i < count; i++) {
        char *ts = ring[i % HISTORY_REPLAY];
        char *who = strchr(ts, '\t');
        if (!who)
            continue;
        *who++ = '\0';
        char *text = strchr(who, '\t');
        if (!text)
            continue;
        *text++ = '\0';
        if (strcmp(who, "*") == 0)
            send_text(c, "[%s] *** %s", ts, text);
        else
            send_text(c, "[%s] %s: %s", ts, who, text);
        if (c->dead)
            return;
    }
    send_text(c, "*** --- het lich su ---");
}

/* ------------------------------------------------------------------ */
/* Xac thuc                                                            */
/* ------------------------------------------------------------------ */

static int split_cred(char *args, char **user, char **pass)
{
    while (*args == ' ')
        args++;
    *user = args;
    char *sp = strchr(args, ' ');
    if (!sp)
        return -1;
    *sp++ = '\0';
    while (*sp == ' ')
        sp++;
    *pass = sp;
    return (**user && **pass) ? 0 : -1;
}

static void auth_fail(struct client *c, const char *reason)
{
    c->auth_fails++;
    log_msg("fd=%d xac thuc that bai (%d/%d): %s",
            c->h.fd, c->auth_fails, MAX_AUTH_FAILS, reason);
    send_text(c, "-ERR %s", reason);
    if (c->auth_fails >= MAX_AUTH_FAILS)
        kill_client(c, "qua nhieu lan xac thuc sai");
}

static void complete_login(struct client *c, const char *name)
{
    c->authed = 1;
    c->auth_fails = 0;
    snprintf(c->nick, sizeof c->nick, "%.*s", NICK_MAX, name);
    log_msg("fd=%d (%s) dang nhap thanh cong [%d online]",
            c->h.fd, c->nick, count_authed());

    send_text(c, "+OK %s", c->nick);
    send_text(c, "*** Chao mung %s! Go /help de xem danh sach lenh.", c->nick);
    send_history(c);
    if (c->dead)
        return;
    hist_log("*", "%s vao phong", c->nick);
    broadcast(c, "*** %s vua vao phong", c->nick);
}

static void handle_auth(struct client *c, char *msg)
{
    int reg;
    char *args;

    if (strncmp(msg, "/login ", 7) == 0) {
        reg = 0;
        args = msg + 7;
    } else if (strncmp(msg, "/register ", 10) == 0) {
        reg = 1;
        args = msg + 10;
    } else {
        send_text(c, "-ERR Chua dang nhap. Dung /login <ten> <mat khau> "
                     "hoac /register <ten> <mat khau>");
        return;
    }

    char *user, *pass;
    if (split_cred(args, &user, &pass) < 0) {
        auth_fail(c, "Cu phap: /login <ten> <mat khau>");
        return;
    }

    if (reg) {
        size_t pl = strlen(pass);
        if (!valid_name(user)) {
            auth_fail(c, "Ten dang nhap khong hop le (A-Z a-z 0-9 _, toi da 16 ky tu)");
            return;
        }
        if (pl < PASS_MIN || pl > PASS_MAX) {
            auth_fail(c, "Mat khau phai dai tu 6 den 72 ky tu");
            return;
        }
        if (acc_find(user)) {
            auth_fail(c, "Ten dang nhap da ton tai");
            return;
        }
        if (add_user(user, pass) < 0) {
            log_msg("fd=%d khong tao duoc tai khoan '%s'", c->h.fd, user);
            send_text(c, "-ERR Loi may chu khi tao tai khoan");
            return;
        }
        log_msg("fd=%d dang ky tai khoan moi: %s", c->h.fd, user);
    } else {
        struct account *a = acc_find(user);
        int ok = verify_pass(pass, a ? a->hash : g_dummy_hash);
        if (!a || !ok) {
            auth_fail(c, "Sai ten dang nhap hoac mat khau");
            return;
        }
        if (find_by_nick(a->name)) {
            send_text(c, "-ERR Tai khoan dang duoc dung o noi khac");
            return;
        }
    }
    complete_login(c, user);
}

/* ------------------------------------------------------------------ */
/* Xu ly lenh / tin nhan                                               */
/* ------------------------------------------------------------------ */

static void handle_message(struct client *c, char *msg)
{
    size_t n = strlen(msg);
    while (n > 0 && isspace((unsigned char)msg[n - 1]))
        msg[--n] = '\0';
    if (n == 0)
        return;

    if (!c->authed) {
        handle_auth(c, msg);
        return;
    }

    if (msg[0] != '/') {
        log_msg("[%s] %s", c->nick, msg);
        hist_log(c->nick, "%s", msg);
        broadcast(NULL, "[%s] %s", c->nick, msg);
        g_total_messages++;
        return;
    }

    if (strcmp(msg, "/list") == 0) {
        send_text(c, "*** %d nguoi dang online:", count_authed());
        for (struct client *o = g_clients; o; o = o->next)
            if (!o->dead && o->authed)
                send_text(c, "  - %s%s%s", o->nick, o->is_unix ? " (unix)" : " (tcp)",
                          o == c ? " <- ban" : "");
    } else if (strncmp(msg, "/msg ", 5) == 0) {
        char *to = msg + 5;
        while (*to == ' ') to++;
        char *text = strchr(to, ' ');
        if (!text) {
            send_text(c, "!! cu phap: /msg <ten> <noi dung>");
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
    } else if (strcmp(msg, "/whoami") == 0) {
        time_t now = time(NULL);
        long online_sec = (long)(now - c->connected_at);
        send_text(c, "*** Thong tin phien cua ban:");
        send_text(c, "    - Username: %s", c->nick);
        send_text(c, "    - Kenh ket noi: %s", c->is_unix ? "Unix domain socket" : "TCP");
        send_text(c, "    - Dinh danh: %s", c->peer_info);
        send_text(c, "    - Thoi gian da online: %ld giay", online_sec);
    } else if (strcmp(msg, "/stats") == 0) {
        time_t now = time(NULL);
        long up = (long)(now - g_start_time);
        send_text(c, "*** Thong ke trang thai may chu:");
        send_text(c, "    - Thoi gian chay (uptime): %ld gio %ld phut %ld giay",
                  up / 3600, (up % 3600) / 60, up % 60);
        send_text(c, "    - Ket noi dang mo: %d | User dang online: %d",
                  g_nclients, count_authed());
        send_text(c, "    - Tong so tai khoan da dang ky: %zu", g_nacc);
        send_text(c, "    - Tong tin nhan broadcast: %llu", g_total_messages);
    } else if (strncmp(msg, "/passwd ", 8) == 0) {
        char *args = msg + 8;
        char *oldp, *newp;
        if (split_cred(args, &oldp, &newp) < 0) {
            send_text(c, "!! Cu phap: /passwd <mat_khau_cu> <mat_khau_moi>");
            return;
        }
        struct account *a = acc_find(c->nick);
        if (!a || !verify_pass(oldp, a->hash)) {
            send_text(c, "-ERR Mat khau cu khong dung");
            explicit_bzero(oldp, strlen(oldp));
            explicit_bzero(newp, strlen(newp));
            return;
        }
        size_t nl = strlen(newp);
        if (nl < PASS_MIN || nl > PASS_MAX) {
            send_text(c, "-ERR Mat khau moi phai tu %d den %d ky tu", PASS_MIN, PASS_MAX);
            explicit_bzero(oldp, strlen(oldp));
            explicit_bzero(newp, strlen(newp));
            return;
        }
        if (update_user_password(c->nick, newp) < 0) {
            send_text(c, "-ERR Loi may chu khi cap nhat mat khau");
        } else {
            log_msg("fd=%d (%s) da doi mat khau thanh cong", c->h.fd, c->nick);
            send_text(c, "+OK Doi mat khau thanh cong!");
        }
        explicit_bzero(oldp, strlen(oldp));
        explicit_bzero(newp, strlen(newp));
    } else if (strcmp(msg, "/help") == 0) {
        send_text(c, "*** Cac lenh ho tro: /list | /msg <ten> <nd> | /whoami | /stats | /passwd <cu> <moi> | /quit | /help");
    } else if (strcmp(msg, "/quit") == 0) {
        send_text(c, "*** tam biet!");
        kill_client(c, "/quit");
    } else {
        send_text(c, "!! lenh khong ro, go /help de xem danh sach");
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

    size_t off = 0;
    while (c->rlen - off >= HEADER_SIZE) {
        uint32_t len;
        memcpy(&len, c->rbuf + off, HEADER_SIZE);
        len = ntohl(len);

        if (len == 0 || len > MAX_PAYLOAD) {
            kill_client(c, "frame khong hop le (sai do dai)");
            return;
        }
        if (c->rlen - off < HEADER_SIZE + (size_t)len)
            break;

        char msg[MAX_PAYLOAD + 1];
        memcpy(msg, c->rbuf + off + HEADER_SIZE, len);
        msg[len] = '\0';
        off += HEADER_SIZE + (size_t)len;

        handle_message(c, msg);
        explicit_bzero(msg, sizeof msg);
        if (c->dead)
            return;
    }

    if (off) {
        memmove(c->rbuf, c->rbuf + off, c->rlen - off);
        c->rlen -= off;
        explicit_bzero(c->rbuf + c->rlen, off);
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
        c->connected_at = time(NULL);
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
            } else if (ss.ss_family == AF_INET6) {
                struct sockaddr_in6 *a6 = (struct sockaddr_in6 *)&ss;
                if (IN6_IS_ADDR_V4MAPPED(&a6->sin6_addr)) {
                    struct in_addr a4;
                    memcpy(&a4, &a6->sin6_addr.s6_addr[12], sizeof a4);
                    inet_ntop(AF_INET, &a4, ip, sizeof ip);
                    snprintf(who, sizeof who, "tcp %s:%u (v4)", ip, ntohs(a6->sin6_port));
                } else {
                    inet_ntop(AF_INET6, &a6->sin6_addr, ip, sizeof ip);
                    snprintf(who, sizeof who, "tcp [%s]:%u", ip, ntohs(a6->sin6_port));
                }
            } else {
                snprintf(who, sizeof who, "tcp");
            }
        }
        snprintf(c->peer_info, sizeof c->peer_info, "%s", who);

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

        c->next = g_clients;
        if (g_clients)
            g_clients->prev = c;
        g_clients = c;
        g_nclients++;

        log_msg("fd=%d ket noi (%s), cho dang nhap [co %d ket noi]",
                fd, who, g_nclients);
        send_text(c, "*** Can dang nhap: /login <ten> <mat khau> "
                     "hoac /register <ten> <mat khau>");
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

    unlink(path);
    memset(&addr, 0, sizeof addr);
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path);
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0)
        die("bind(unix)");
    chmod(path, 0666);
    if (listen(fd, SOMAXCONN) < 0)
        die("listen(unix)");
    return fd;
}

static int make_tcp_listener(uint16_t port)
{
    /* Uu tien socket dual-stack IPv6 neu ho tro */
    int fd = socket(AF_INET6, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd >= 0) {
        int one = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
        int zero = 0;
        setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &zero, sizeof zero);

        struct sockaddr_in6 addr6;
        memset(&addr6, 0, sizeof addr6);
        addr6.sin6_family = AF_INET6;
        addr6.sin6_addr = in6addr_any;
        addr6.sin6_port = htons(port);
        if (bind(fd, (struct sockaddr *)&addr6, sizeof addr6) == 0) {
            if (listen(fd, SOMAXCONN) == 0)
                return fd;
        }
        close(fd);
    }

    /* Fallback sang IPv4 thuan neu he thong khong ho tro IPv6 */
    fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
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

static int parse_port(const char *s)
{
    char *end;
    long p = strtol(s, &end, 10);
    if (*s == '\0' || *end != '\0' || p <= 0 || p > 65535)
        return -1;
    return (int)p;
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "dung: %s [-u unix_path] [-p port] [-d users.db] [-l chat.log]\n",
            prog);
}

int main(int argc, char **argv)
{
    int port = DEFAULT_TCP_PORT;

    int opt;
    while ((opt = getopt(argc, argv, "u:p:d:l:h")) != -1) {
        switch (opt) {
        case 'u': g_unix_path = optarg;  break;
        case 'd': g_users_path = optarg; break;
        case 'l': g_hist_path = optarg;  break;
        case 'p':
            port = parse_port(optarg);
            if (port < 0) {
                usage(argv[0]);
                return EXIT_FAILURE;
            }
            break;
        default:
            usage(argv[0]);
            return EXIT_FAILURE;
        }
    }
    if (optind < argc)
        g_unix_path = argv[optind];
    if (optind + 1 < argc) {
        port = parse_port(argv[optind + 1]);
        if (port < 0) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    load_users();
    g_hist_fd = open(g_hist_path, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0600);
    if (g_hist_fd < 0)
        die(g_hist_path);
    tighten_perms(g_hist_fd, g_hist_path);
    if (make_hash("mat-khau-gia", g_dummy_hash, sizeof g_dummy_hash) < 0) {
        fprintf(stderr, "khong tao duoc bam mat khau (crypt/getrandom)\n");
        return EXIT_FAILURE;
    }

    g_ep = epoll_create1(EPOLL_CLOEXEC);
    if (g_ep < 0)
        die("epoll_create1");

    g_lunix.fd = make_unix_listener(g_unix_path);
    g_ltcp.fd  = make_tcp_listener((uint16_t)port);
    add_listener(&g_lunix);
    add_listener(&g_ltcp);

    g_start_time = time(NULL);

    log_msg("server san sang: unix=%s tcp=:%d users=%s lich_su=%s (Ctrl+C de dung)",
            g_unix_path, port, g_users_path, g_hist_path);

    struct epoll_event evs[MAX_EVENTS];
    while (!g_stop) {
        int n = epoll_wait(g_ep, evs, MAX_EVENTS, 1000);
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

        reap_unauthed();
        sweep_dead();
    }

    /* Graceful Shutdown: Thong bao truoc khi dong ket noi */
    log_msg("dang tat server, thong bao cho tat ca client...");
    broadcast(NULL, "*** May chu dang tat de bao tri (shutdown). Tam biet!");
    for (struct client *c = g_clients; c; c = c->next) {
        if (!c->dead) {
            flush_client(c);
            close(c->h.fd);
        }
    }
    while (g_clients) {
        struct client *next = g_clients->next;
        explicit_bzero(g_clients->rbuf, sizeof g_clients->rbuf);
        free(g_clients->wbuf);
        free(g_clients);
        g_clients = next;
    }
    for (size_t i = 0; i < g_nacc; i++)
        free(g_acc[i].hash);
    free(g_acc);
    close(g_lunix.fd);
    close(g_ltcp.fd);
    close(g_ep);
    close(g_users_fd);
    close(g_hist_fd);
    unlink(g_unix_path);
    log_msg("server da tat hoan toan.");
    return EXIT_SUCCESS;
}
