/*
 * proto.h - Dinh nghia giao thuc va cac ham xu ly frame dung chung
 *           cho ca server va client.
 *
 * Giao thuc frame: [4 byte do dai, big-endian][payload]
 *   - len: uint32_t tinh theo byte cua payload (1 <= len <= MAX_PAYLOAD)
 *   - payload: chuoi ky tu UTF-8, khong co byte \0 o cuoi
 */
#ifndef PROTO_H
#define PROTO_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define MAX_PAYLOAD        4096         /* Payload toi da cua 1 frame */
#define HEADER_SIZE        4            /* Kich thuoc header do dai (uint32) */
#define RBUF_SIZE          (HEADER_SIZE + MAX_PAYLOAD) /* 4100 byte */
#define WBUF_LIMIT         (1 << 20)    /* 1 MiB: gioi han hang doi ghi */

#define NICK_MAX           16           /* Do dai toi da cua username */
#define PASS_MIN           6            /* Do dai toi thieu cua mat khau */
#define PASS_MAX           72           /* Do dai toi da cua mat khau */
#define MAX_AUTH_FAILS     3            /* So lan xac thuc sai toi da */
#define AUTH_TIMEOUT_SEC   30           /* Thoi gian cho xac thuc toi da (giay) */
#define HISTORY_REPLAY     20           /* So dong lich su phat lai khi dang nhap */
#define HISTORY_TAIL_BYTES 32768        /* Dung luong doc duoi chat.log */

/*
 * Ghi du n byte vao fd (blocking), tu dong thu lai khi bi EINTR hoac short write.
 * Tra ve 0 neu thanh cong, -1 neu gap loi.
 */
int write_full(int fd, const void *buf, size_t n);

/*
 * Doc dung n byte tu fd (blocking), tu dong thu lai khi bi EINTR hoac short read.
 * Tra ve 0 neu du n byte, -1 neu EOF hoac loi.
 */
int read_exact(int fd, void *buf, size_t n);

/*
 * Dong goi mot frame vao buffer: [4 byte do dai big-endian][payload]
 * Tra ve tong so byte da ghi (4 + len), hoac -1 neu len khong hop le.
 */
ssize_t pack_frame(uint8_t *dst, size_t dst_cap, const void *data, size_t len);

/*
 * Gui truc tiep 1 frame qua socket fd bang write_full.
 * Tra ve 0 neu thanh cong, -1 neu loi.
 */
int send_frame_blocking(int fd, const void *data, size_t len);

/*
 * Doc 1 frame hoan chinh tu socket blocking fd (dung khi client xac thuc).
 * Tra ve do dai payload neu thanh cong, -1 neu loi hoac frame sai.
 * Buffer out_buf se duoc tu dong them ky tu \0 o cuoi.
 */
int read_frame_blocking(int fd, char *out_buf, size_t cap);

#endif /* PROTO_H */
