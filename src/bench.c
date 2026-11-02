/*
 * bench.c - So sanh do tre va thong luong cua cac co che IPC tren Linux
 *
 *   pipe        : 2 pipe (mot chieu moi pipe)
 *   unix_stream : AF_UNIX, SOCK_STREAM
 *   unix_dgram  : AF_UNIX, SOCK_DGRAM
 *   unix_seqp   : AF_UNIX, SOCK_SEQPACKET
 *   tcp         : TCP qua loopback 127.0.0.1
 *   shm         : POSIX Shared Memory (shm_open + mmap + POSIX Semaphore)
 *
 * Moi cap (co che, kich thuoc) do 2 chi so:
 *   1. Do tre: ping-pong (cha gui S byte, con gui tra S byte), do RTT bang
 *      clock_gettime(CLOCK_MONOTONIC); bo qua cac vong warm-up; bao cao
 *      do tre mot chieu = RTT/2 (trung binh, p50, p99).
 *   2. Thong luong: cha gui lien tuc N goi S byte, con doc het;
 *      thoi gian tinh tu luc bat dau gui den luc hoan thanh.
 *
 * Ket qua in ra stdout dang CSV; tien do in ra stderr.
 *
 * Build:  gcc -Wall -Wextra -O2 -o bench bench.c -pthread -lrt
 * Chay:   ./bench > results.csv
 *         ./bench -m tcp -m shm -a 0 -b 1 > results.csv
 * Tuy chon:
 *   -m <co che>   chi chay co che nay (lap lai duoc); mac dinh: tat ca
 *   -a <cpu>      ghim tien trinh cha vao CPU nay      (mac dinh 0)
 *   -b <cpu>      ghim tien trinh con vao CPU nay      (mac dinh 1)
 *   -n            khong ghim CPU
 *   -q            che do nhanh (it vong lap, de thu)
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sched.h>
#include <semaphore.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

enum mech { M_PIPE, M_USTREAM, M_UDGRAM, M_USEQP, M_TCP, M_SHM, M_COUNT };

static const char *mech_name[M_COUNT] = {
    "pipe", "unix_stream", "unix_dgram", "unix_seqp", "tcp", "shm"
};

static const size_t sizes[] = { 64, 1024, 4096, 16384, 65536 };
#define NSIZES (sizeof sizes / sizeof sizes[0])

#define SINK_BUF (1 << 20)   /* buffer doc cua ben nhan khi do thong luong */

static int g_quick = 0;

struct ep { int rd, wr; };   /* dau doc / dau ghi cua mot tien trinh */

/* Cau truc vung bo nho chia se POSIX Shared Memory */
struct shm_area {
    sem_t sem_p2c;           /* Cha bao cho con */
    sem_t sem_c2p;           /* Con bao cho cha */
    char data[65536];        /* Vung dem truyen du lieu (toi da 64 KiB) */
};

static struct shm_area *g_shm = NULL;

/* ------------------------------------------------------------------ */

static void die(const char *what)
{
    perror(what);
    exit(EXIT_FAILURE);
}

static double now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

static int is_stream(int m)
{
    return m == M_PIPE || m == M_USTREAM || m == M_TCP;
}

static void pin_cpu(int cpu)
{
    if (cpu < 0)
        return;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (sched_setaffinity(0, sizeof set, &set) < 0)
        perror("sched_setaffinity (bo qua)");
}

/* ------------------------------------------------------------------ */
/* I/O co xu ly short read/write                                       */
/* ------------------------------------------------------------------ */

static void write_full(int fd, const char *buf, size_t n)
{
    size_t done = 0;
    while (done < n) {
        ssize_t w = write(fd, buf + done, n - done);
        if (w < 0) {
            if (errno == EINTR)
                continue;
            die("write");
        }
        done += (size_t)w;
    }
}

static void read_full(int fd, char *buf, size_t n)
{
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, buf + done, n - done);
        if (r == 0) {
            fprintf(stderr, "ben kia dong ket noi som\n");
            exit(EXIT_FAILURE);
        }
        if (r < 0) {
            if (errno == EINTR)
                continue;
            die("read");
        }
        done += (size_t)r;
    }
}

static void recv_msg(int m, int fd, char *buf, size_t s)
{
    if (is_stream(m)) {
        read_full(fd, buf, s);
    } else {
        ssize_t r;
        do {
            r = read(fd, buf, s);
        } while (r < 0 && errno == EINTR);
        if (r < 0)
            die("read(msg)");
        if ((size_t)r != s) {
            fprintf(stderr, "datagram bi cat: %zd/%zu\n", r, s);
            exit(EXIT_FAILURE);
        }
    }
}

/* ------------------------------------------------------------------ */
/* Dung kenh giao tiep                                                 */
/* ------------------------------------------------------------------ */

static void setup_tcp(struct ep *par, struct ep *chi)
{
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    if (ls < 0)
        die("socket");

    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;
    if (bind(ls, (struct sockaddr *)&a, sizeof a) < 0)
        die("bind");
    if (listen(ls, 1) < 0)
        die("listen");
    socklen_t al = sizeof a;
    if (getsockname(ls, (struct sockaddr *)&a, &al) < 0)
        die("getsockname");

    int c = socket(AF_INET, SOCK_STREAM, 0);
    if (c < 0)
        die("socket");
    if (connect(c, (struct sockaddr *)&a, sizeof a) < 0)
        die("connect");
    int s = accept(ls, NULL, NULL);
    if (s < 0)
        die("accept");
    close(ls);

    int one = 1;
    setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

    par->rd = par->wr = c;
    chi->rd = chi->wr = s;
}

static void setup_shm(void)
{
    int fd = shm_open("/ltht_bench_shm", O_CREAT | O_RDWR, 0600);
    if (fd < 0)
        die("shm_open");
    if (ftruncate(fd, sizeof(struct shm_area)) < 0)
        die("ftruncate");
    g_shm = mmap(NULL, sizeof(struct shm_area), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (g_shm == MAP_FAILED)
        die("mmap");
    shm_unlink("/ltht_bench_shm");
    close(fd);

    if (sem_init(&g_shm->sem_p2c, 1, 0) < 0)
        die("sem_init p2c");
    if (sem_init(&g_shm->sem_c2p, 1, 0) < 0)
        die("sem_init c2p");
}

static void setup(int m, struct ep *par, struct ep *chi)
{
    int sv[2];
    switch (m) {
    case M_PIPE: {
        int p2c[2], c2p[2];
        if (pipe(p2c) < 0 || pipe(c2p) < 0)
            die("pipe");
        par->rd = c2p[0]; par->wr = p2c[1];
        chi->rd = p2c[0]; chi->wr = c2p[1];
        break;
    }
    case M_USTREAM:
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0)
            die("socketpair");
        par->rd = par->wr = sv[0];
        chi->rd = chi->wr = sv[1];
        break;
    case M_UDGRAM:
        if (socketpair(AF_UNIX, SOCK_DGRAM, 0, sv) < 0)
            die("socketpair");
        par->rd = par->wr = sv[0];
        chi->rd = chi->wr = sv[1];
        break;
    case M_USEQP:
        if (socketpair(AF_UNIX, SOCK_SEQPACKET, 0, sv) < 0)
            die("socketpair");
        par->rd = par->wr = sv[0];
        chi->rd = chi->wr = sv[1];
        break;
    case M_TCP:
        setup_tcp(par, chi);
        break;
    case M_SHM:
        setup_shm();
        break;
    }
}

/* ------------------------------------------------------------------ */
/* Tien trinh con: echo (do tre) roi sink (thong luong)                */
/* ------------------------------------------------------------------ */

static void child_main(int m, struct ep e, size_t s, int lat_total, long thr_n)
{
    char *buf = malloc(SINK_BUF);
    if (!buf)
        die("malloc");

    if (m == M_SHM) {
        /* Pha 1: echo do tre */
        for (int i = 0; i < lat_total; i++) {
            while (sem_wait(&g_shm->sem_p2c) < 0) {
                if (errno == EINTR) continue;
                die("sem_wait");
            }
            memcpy(buf, g_shm->data, s);
            memcpy(g_shm->data, buf, s);
            sem_post(&g_shm->sem_c2p);
        }
        /* Pha 2: sink thong luong */
        for (long i = 0; i < thr_n; i++) {
            while (sem_wait(&g_shm->sem_p2c) < 0) {
                if (errno == EINTR) continue;
                die("sem_wait");
            }
            memcpy(buf, g_shm->data, s);
            sem_post(&g_shm->sem_c2p);
        }
        free(buf);
        return;
    }

    /* pha 1: echo */
    for (int i = 0; i < lat_total; i++) {
        recv_msg(m, e.rd, buf, s);
        write_full(e.wr, buf, s);
    }

    /* pha 2: sink - doc het thr_n * s byte roi ack */
    if (is_stream(m)) {
        size_t want = (size_t)thr_n * s, got = 0;
        while (got < want) {
            size_t chunk = want - got < SINK_BUF ? want - got : SINK_BUF;
            ssize_t r = read(e.rd, buf, chunk);
            if (r == 0)
                break;
            if (r < 0) {
                if (errno == EINTR)
                    continue;
                die("read(sink)");
            }
            got += (size_t)r;
        }
    } else {
        for (long i = 0; i < thr_n; i++)
            recv_msg(m, e.rd, buf, s);
    }
    char ack = 'A';
    write_full(e.wr, &ack, 1);
    free(buf);
}

/* ------------------------------------------------------------------ */
/* Tien trinh cha: do                                                  */
/* ------------------------------------------------------------------ */

static int cmp_d(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static void run_one(int m, size_t s, int cpu_a, int cpu_b)
{
    int warm = g_quick ? 100 : 2000;
    int iters = g_quick ? 1000 : (s <= 4096 ? 20000 : 5000);
    long thr_n = 128L * 1024 * 1024 / (long)s;
    if (thr_n > 100000)
        thr_n = 100000;
    if (g_quick)
        thr_n /= 20;
    if (thr_n < 100)
        thr_n = 100;

    struct ep par = { -1, -1 }, chi = { -1, -1 };
    setup(m, &par, &chi);

    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0)
        die("fork");
    if (pid == 0) {
        pin_cpu(cpu_b);
        if (m != M_SHM) {
            close(par.rd);
            if (par.wr != par.rd)
                close(par.wr);
        }
        child_main(m, chi, s, warm + iters, thr_n);
        _exit(EXIT_SUCCESS);
    }
    pin_cpu(cpu_a);
    if (m != M_SHM) {
        close(chi.rd);
        if (chi.wr != chi.rd)
            close(chi.wr);
    }

    char *buf = malloc(s > 1 ? s : 1);
    double *rtt = malloc(sizeof(double) * (size_t)iters);
    if (!buf || !rtt)
        die("malloc");
    memset(buf, 0x5a, s);

    if (m == M_SHM) {
        /* --- do tre SHM --- */
        for (int i = 0; i < warm + iters; i++) {
            double t0 = now_us();
            memcpy(g_shm->data, buf, s);
            sem_post(&g_shm->sem_p2c);
            while (sem_wait(&g_shm->sem_c2p) < 0) {
                if (errno == EINTR) continue;
                die("sem_wait");
            }
            memcpy(buf, g_shm->data, s);
            double t1 = now_us();
            if (i >= warm)
                rtt[i - warm] = t1 - t0;
        }
        double sum = 0;
        for (int i = 0; i < iters; i++)
            sum += rtt[i];
        qsort(rtt, (size_t)iters, sizeof(double), cmp_d);
        double mean = sum / iters / 2.0;
        double p50  = rtt[iters / 2] / 2.0;
        double p99  = rtt[(int)(iters * 0.99)] / 2.0;

        /* --- thong luong SHM --- */
        double t0 = now_us();
        for (long i = 0; i < thr_n; i++) {
            memcpy(g_shm->data, buf, s);
            sem_post(&g_shm->sem_p2c);
            while (sem_wait(&g_shm->sem_c2p) < 0) {
                if (errno == EINTR) continue;
                die("sem_wait");
            }
        }
        double t1 = now_us();
        double mbps = ((double)thr_n * (double)s) / (t1 - t0);

        printf("%s,%zu,%.3f,%.3f,%.3f,%.1f\n",
               mech_name[m], s, mean, p50, p99, mbps);
        fflush(stdout);

        sem_destroy(&g_shm->sem_p2c);
        sem_destroy(&g_shm->sem_c2p);
        munmap(g_shm, sizeof(struct shm_area));
        g_shm = NULL;
        free(buf);
        free(rtt);

        int st;
        waitpid(pid, &st, 0);
        return;
    }

    /* --- do tre Socket / Pipe --- */
    for (int i = 0; i < warm + iters; i++) {
        double t0 = now_us();
        write_full(par.wr, buf, s);
        recv_msg(m, par.rd, buf, s);
        double t1 = now_us();
        if (i >= warm)
            rtt[i - warm] = t1 - t0;
    }
    double sum = 0;
    for (int i = 0; i < iters; i++)
        sum += rtt[i];
    qsort(rtt, (size_t)iters, sizeof(double), cmp_d);
    double mean = sum / iters / 2.0;
    double p50  = rtt[iters / 2] / 2.0;
    double p99  = rtt[(int)(iters * 0.99)] / 2.0;

    /* --- thong luong Socket / Pipe --- */
    double t0 = now_us();
    for (long i = 0; i < thr_n; i++)
        write_full(par.wr, buf, s);
    char ack;
    read_full(par.rd, &ack, 1);
    double t1 = now_us();
    double mbps = ((double)thr_n * (double)s) / (t1 - t0);

    printf("%s,%zu,%.3f,%.3f,%.3f,%.1f\n",
           mech_name[m], s, mean, p50, p99, mbps);
    fflush(stdout);

    close(par.rd);
    if (par.wr != par.rd)
        close(par.wr);
    free(buf);
    free(rtt);

    int st;
    waitpid(pid, &st, 0);
    if (!WIFEXITED(st) || WEXITSTATUS(st) != 0) {
        fprintf(stderr, "tien trinh con loi o %s/%zu\n", mech_name[m], s);
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char **argv)
{
    int sel[M_COUNT] = {0}, any = 0;
    int cpu_a = 0, cpu_b = 1;

    int opt;
    while ((opt = getopt(argc, argv, "m:a:b:nq")) != -1) {
        switch (opt) {
        case 'm': {
            int ok = 0;
            for (int i = 0; i < M_COUNT; i++)
                if (strcmp(optarg, mech_name[i]) == 0) {
                    sel[i] = 1; any = ok = 1;
                }
            if (!ok) {
                fprintf(stderr, "co che khong biet: %s\n", optarg);
                return EXIT_FAILURE;
            }
            break;
        }
        case 'a': cpu_a = atoi(optarg); break;
        case 'b': cpu_b = atoi(optarg); break;
        case 'n': cpu_a = cpu_b = -1;   break;
        case 'q': g_quick = 1;          break;
        default:
            fprintf(stderr, "dung: %s [-m co_che]... [-a cpu] [-b cpu] [-n] [-q]\n",
                    argv[0]);
            return EXIT_FAILURE;
        }
    }
    if (!any)
        for (int i = 0; i < M_COUNT; i++)
            sel[i] = 1;

    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    if (cpu_a >= 0 && (cpu_a >= ncpu || cpu_b >= ncpu)) {
        fprintf(stderr, "[canh bao] may chi co %ld CPU, bo ghim CPU\n", ncpu);
        cpu_a = cpu_b = -1;
    }
    signal(SIGPIPE, SIG_IGN);

    fprintf(stderr, "CPU online: %ld | ghim cha=%d con=%d | %s\n",
            ncpu, cpu_a, cpu_b, g_quick ? "QUICK" : "full");

    printf("mech,size,lat_mean_us,lat_p50_us,lat_p99_us,throughput_MBps\n");
    for (int m = 0; m < M_COUNT; m++) {
        if (!sel[m])
            continue;
        for (size_t i = 0; i < NSIZES; i++) {
            fprintf(stderr, "  %-12s %6zu B ...\n", mech_name[m], sizes[i]);
            run_one(m, sizes[i], cpu_a, cpu_b);
        }
    }
    return EXIT_SUCCESS;
}
