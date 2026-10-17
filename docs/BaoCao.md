# Báo cáo: Sockets trong IPC - Chat server đa client bằng epoll

Oct 17, 2026 · @Minh

## Tóm tắt

Đồ án xây dựng một chat server đa client dùng `epoll`, phục vụ đồng thời client qua Unix domain socket và TCP trong một tiến trình, một luồng, kèm chương trình benchmark so sánh năm cơ chế IPC.

Giao thức dùng tiền tố độ dài 4 byte để giữ ranh giới tin nhắn trên dòng byte. Mỗi client có buffer đọc và buffer ghi riêng, nên server ghép được tin nhắn đến thành nhiều mẩu và không bị chặn bởi client đọc chậm. Server được kiểm thử với client Unix và TCP chung một phòng, client bị ngắt đột ngột, frame bị cắt làm đôi; ASan và UBSan không báo lỗi.

Benchmark đo độ trễ ping-pong và thông lượng của pipe, Unix stream, Unix datagram, Unix seqpacket và TCP loopback, ở năm kích thước từ 64 byte đến 64 KiB. Số liệu đo trên máy thật được điền ở mục 5.

## 1. Giới thiệu IPC và vị trí của socket

Socket là cơ chế IPC phổ biến duy nhất chạy được cả trong một máy lẫn giữa các máy, vì vậy đồ án chọn nó làm nền tảng giao tiếp.

IPC (Inter-Process Communication) là tập các cơ chế cho phép các tiến trình, vốn có không gian địa chỉ tách biệt, trao đổi dữ liệu và đồng bộ với nhau thông qua kernel. Bảng dưới so sánh các cơ chế chính trên Linux.

| Cơ chế | Phạm vi | Hướng | Ranh giới tin nhắn | Đặc điểm |
| --- | --- | --- | --- | --- |
| Pipe | Cùng máy, các tiến trình có tổ tiên chung | Một chiều | Không | Đơn giản, thường dùng giữa cha và con |
| FIFO (named pipe) | Cùng máy | Một chiều | Không | Như pipe nhưng có tên trong hệ thống file |
| Message queue (POSIX, SysV) | Cùng máy | Qua hàng đợi | Có | Có độ ưu tiên; giới hạn kích thước theo cấu hình hệ điều hành |
| Shared memory | Cùng máy | Hai chiều | Tự quản lý | Không sao chép dữ liệu nên nhanh nhất, nhưng phải tự đồng bộ |
| Unix domain socket | Cùng máy | Hai chiều | Tùy loại (`STREAM` không, `DGRAM` và `SEQPACKET` có) | Địa chỉ là đường dẫn file; truyền được file descriptor (`SCM_RIGHTS`); lấy được PID và UID đối phương (`SO_PEERCRED`) |
| TCP, UDP socket | Cùng máy hoặc qua mạng | Hai chiều | TCP không, UDP có | Đi qua network stack, dùng được giữa các máy |

Đồ án dùng hai loại socket cùng kiểu `SOCK_STREAM`: `AF_UNIX` và `AF_INET`. Cùng một bộ API (`socket`, `bind`, `listen`, `accept`, `connect`, `read`, `write`) phục vụ cả hai, nên phần khác nhau của server chỉ nằm ở bước tạo và gán địa chỉ cho socket lắng nghe.

Vì `SOCK_STREAM` là dòng byte không giữ ranh giới tin nhắn, giao thức tầng ứng dụng phải tự đóng khung (mục 3). Phần benchmark (mục 5) so sánh thêm hai loại Unix socket có ranh giới tin nhắn là `SOCK_DGRAM` và `SOCK_SEQPACKET`.

## 2. Kiến trúc hệ thống

Server dùng một `epoll` duy nhất để theo dõi hai socket lắng nghe (Unix và TCP) cùng mọi socket client, trong một tiến trình, một luồng.

&#91;embedded content: kiến trúc server · 2 listener, 1 epoll, buffer riêng mỗi client\]

Mọi kết nối, Unix hay TCP, đều đi qua cùng một vòng `epoll`. Sau khi tách frame và xử lý, phản hồi được xếp vào `wbuf` rồi gửi ngược về client bằng `send()`.

### Vòng lặp sự kiện

Mỗi vòng, server gọi `epoll_wait`, xử lý các sự kiện trả về, rồi giải phóng các client đã chết.

| Nguồn | Sự kiện | Xử lý |
| --- | --- | --- |
| Listener | `EPOLLIN` | `accept4` lặp đến `EAGAIN`; tạo `struct client`, `epoll_ctl(ADD)`, gửi lời chào và thông báo "vừa vào phòng" |
| Client | `EPOLLIN` | `read` vào `rbuf`, tách mọi frame đủ, gọi `handle_message` |
| Client | `EPOLLOUT` | `flush_client` gửi tiếp phần còn trong `wbuf`, tắt `EPOLLOUT` khi hết |
| Client | `EPOLLERR`, `EPOLLHUP` | `kill_client` |
| Cuối mỗi vòng | (không có) | `sweep_dead` giải phóng các client đã chết |

### Đường đi của một tin nhắn

1. Client A gửi frame `[len][nội dung]`; `epoll_wait` báo `EPOLLIN` trên socket của A.
2. Server `read` vào `rbuf` của A. Khi đủ frame, `handle_message` nhận ra đây là tin thường vì không bắt đầu bằng `/`.
3. `broadcast` gọi `queue_frame` cho mọi client còn sống, kể cả A: mỗi lần là thêm `[len]["[alice] nội dung"]` vào `wbuf` rồi `send` ngay.
4. Nếu `send` trả `EAGAIN`, phần chưa gửi nằm lại trong `wbuf` và `EPOLLOUT` được bật; khi socket sẵn sàng, `flush_client` gửi tiếp.

## 3. Thiết kế giao thức và lý do cần framing

Giao thức dùng tiền tố độ dài vì TCP và `SOCK_STREAM` chỉ đảm bảo thứ tự byte, không đảm bảo ranh giới giữa các lần gửi.

Một lần `write()` 20 byte ở phía gửi có thể đến phía nhận thành hai lần `read()` 8 và 12 byte, và hai lần `write()` liên tiếp có thể gộp thành một lần `read()`. Nếu coi mỗi `read()` là một tin nhắn, ứng dụng sẽ ghép sai hoặc cắt dở tin. Vì vậy cần một quy ước để biết tin nhắn kết thúc ở đâu.

Ba cách đóng khung phổ biến là ký tự phân cách (dấu xuống dòng), độ dài cố định và tiền tố độ dài. Đồ án chọn tiền tố độ dài vì payload là văn bản tự do có thể chứa mọi ký tự (không phải escape), và bên nhận biết trước cần đọc bao nhiêu byte.

### Cấu trúc frame

| Offset | Độ dài | Trường | Mô tả |
| --- | --- | --- | --- |
| 0 | 4 byte | `len` | Số nguyên không dấu 32 bit, big-endian (`htonl`, `ntohl`); chỉ tính độ dài payload |
| 4 | `len` byte | `payload` | Văn bản, không có ký tự kết thúc `\0`; 1 ≤ `len` ≤ 4096 |

### Quy tắc bên nhận

1. Gom byte vào buffer riêng của từng client.
2. Khi đủ 4 byte, đọc `len`. Nếu `len` bằng 0 hoặc lớn hơn 4096 thì ngắt kết nối, để client không ép được server cấp phát bộ nhớ bằng độ dài giả.
3. Khi đủ `4 + len` byte, tách frame, xử lý, rồi dịch phần dư về đầu buffer.
4. Lặp lại bước 2 vì một lần `read()` có thể chứa nhiều frame.

Phía gửi phải xử lý ghi thiếu: client gộp header và payload thành một lần `send()` trong vòng lặp; server đưa frame vào hàng đợi ghi và gửi tiếp khi socket sẵn sàng (mục 4).

Kiểm tra độ dài có tác dụng thực tế: khi gửi văn bản thô `hello` kèm xuống dòng, 4 byte đầu `hell` được đọc thành độ dài khoảng 1,75 tỷ, server ghi log "frame không hợp lệ" và ngắt kết nối.

### Lệnh

| Nhập | Tác dụng |
| --- | --- |
| Nội dung bất kỳ | Gửi cho mọi người (broadcast) |
| `/nick <tên>` | Đổi tên; chỉ gồm `A-Z a-z 0-9 _`, tối đa 16 ký tự, không trùng người khác |
| `/list` | Liệt kê người đang online kèm loại kết nối (`unix` hoặc `tcp`) |
| `/msg <tên> <nội dung>` | Tin nhắn riêng |
| `/help` | Xem danh sách lệnh |
| `/quit` | Thoát |

## 4. Cài đặt

Server là một vòng lặp `epoll_wait` duy nhất và mọi thao tác trên socket đều non-blocking, nên một client chậm không chặn được các client khác.

### System call chính

| Nhóm | System call | Mục đích |
| --- | --- | --- |
| Tạo socket lắng nghe | `socket`, `bind`, `listen`, `setsockopt`, `unlink`, `chmod` | `AF_UNIX` bind vào đường dẫn, `unlink` file cũ trước khi bind và khi thoát. TCP đặt `SO_REUSEADDR` để chạy lại ngay mà không bị `TIME_WAIT` chặn cổng |
| Nhận kết nối | `accept4` với `SOCK_NONBLOCK`, `SOCK_CLOEXEC` | Nhận kết nối và đặt non-blocking, close-on-exec trong một lời gọi; lặp đến khi gặp `EAGAIN` |
| Đa hợp sự kiện | `epoll_create1`, `epoll_ctl` (ADD, MOD, DEL), `epoll_wait` | Theo dõi mọi fd; MOD dùng để bật, tắt `EPOLLOUT` |
| Đọc, ghi | `read`, `send` với `MSG_NOSIGNAL` | Nhận dữ liệu; gửi dữ liệu mà không sinh `SIGPIPE` khi đối phương đã đóng |
| Thông tin đối phương | `getsockopt` (`SO_PEERCRED`), `inet_ntop` | Ghi log PID, UID của client Unix và IP, cổng của client TCP |
| Tối ưu TCP | `setsockopt` (`TCP_NODELAY`) | Tắt thuật toán Nagle để tin nhắn nhỏ đi ngay |
| Tín hiệu | `sigaction`, `signal` | `SIGINT`, `SIGTERM` đặt cờ dừng; `SIGPIPE` bị bỏ qua |
| Client | `poll`, `getaddrinfo`, `connect`, `shutdown` | Chờ đồng thời stdin và socket; kết nối IPv4 hoặc IPv6; báo server khi stdin hết |

### Cấu trúc dữ liệu

Mọi đối tượng đăng ký vào epoll bắt đầu bằng `struct handle { kind, fd }`. Con trỏ `data.ptr` của sự kiện trỏ tới handle này, nên chỉ cần đọc `kind` là biết đó là listener Unix, listener TCP hay client; với client thì ép kiểu về `struct client`.

`struct client` giữ tên, loại kết nối, cờ `dead`, buffer đọc (`rbuf` 4100 byte cùng `rlen`), hàng đợi ghi cấp phát động (`wbuf`, `woff`, `wlen`, `wcap`) và con trỏ `prev`, `next` của danh sách liên kết dùng để broadcast.

### Buffer đọc

`on_readable()` gọi `read()` một lần cho mỗi sự kiện (level-triggered), dồn byte vào `rbuf`, tách mọi frame đã đủ rồi `memmove` phần dư về đầu buffer. Một frame đến làm hai mẩu, 3 byte rồi 2 byte cách nhau 0,2 giây, vẫn được ghép đúng khi thử.

### Buffer ghi

`queue_frame()` thêm `[len][payload]` vào `wbuf` rồi gọi `flush_client()` ngay. Nếu `send()` trả `EAGAIN`, phần chưa gửi (từ `woff` đến `wlen`) được giữ lại và server bật `EPOLLOUT` bằng `epoll_ctl(MOD)`. Khi socket ghi được, `epoll_wait` báo `EPOLLOUT`, server gửi tiếp và tắt `EPOLLOUT` khi hết dữ liệu.

Nếu phần chưa gửi của một client vượt 1 MiB (client đọc quá chậm), server ngắt client đó để bộ nhớ không phình vô hạn.

### Xóa client an toàn

`kill_client()` gỡ fd khỏi epoll, `close(fd)`, đánh dấu `dead` và báo cho những người còn lại, nhưng **chưa** `free`. Lý do: mảng sự kiện của `epoll_wait` hoặc vòng lặp `broadcast` có thể còn giữ con trỏ tới client vừa chết, và `free` sớm gây use-after-free. `sweep_dead()` ở cuối mỗi vòng lặp mới thực sự giải phóng.

### Lỗi và tín hiệu

- `read()` trả 0 là đối phương đóng kết nối; `send()` trả `EPIPE` hoặc `ECONNRESET` thì ngắt client. `EINTR` được thử lại.
- `accept4` gặp `EMFILE` (hết file descriptor) thì ghi log và tiếp tục chạy.
- `SIGINT`, `SIGTERM` chỉ đặt cờ `g_stop`. `sigaction` không dùng `SA_RESTART` nên `epoll_wait` trả `EINTR`, vòng lặp thoát, server đóng mọi kết nối, giải phóng bộ nhớ và `unlink` file socket.
- Client đọc stdin bằng `read()` và tự tách dòng, không dùng stdio, vì dữ liệu nằm trong buffer của stdio sẽ không được `poll()` nhìn thấy.

### Kiểm thử

Server và client được biên dịch với `-Wall -Wextra -fsanitize=address,undefined` không có cảnh báo. Kiểm thử tự động gồm: client Unix và TCP chat chung phòng, `/nick`, `/msg`, `/list`, frame bị cắt làm đôi, client TCP ngắt đột ngột, `/quit`, và tắt server bằng `SIGTERM` (exit code 0, file socket được dọn).

## 5. Benchmark

Benchmark so sánh độ trễ và thông lượng của năm cơ chế IPC: pipe, Unix stream, Unix datagram, Unix seqpacket và TCP loopback. Các bảng dưới để điền số đo trên máy thật, vì số đo trên máy ảo không đại diện cho phần cứng thật.

### Phương pháp đo

- **Độ trễ**: ping-pong. Tiến trình cha gửi `S` byte, tiến trình con gửi lại `S` byte. Đo RTT bằng `clock_gettime(CLOCK_MONOTONIC)`, bỏ 2000 vòng warm-up, rồi đo 20000 vòng (với `S` ≤ 4 KiB) hoặc 5000 vòng (với `S` lớn hơn). Độ trễ một chiều bằng RTT chia 2; báo cáo trung bình, p50 và p99.
- **Thông lượng**: cha gửi liên tục một chiều, con đọc hết rồi trả ack 1 byte; thời gian tính từ lúc bắt đầu gửi đến lúc nhận ack. Mỗi lần gửi tối đa 128 MiB hoặc 300000 gói, tùy mốc nào đạt trước. Đơn vị MB/s với 1 MB = 10^6 byte.
- **Kích thước** `S`: 64 B, 1 KiB, 4 KiB, 16 KiB, 64 KiB.
- **Kênh**: `pipe()`, `socketpair()` và (với TCP) `bind`, `listen`, `connect`, `accept` trên `127.0.0.1` có `TCP_NODELAY`. Đường dữ liệu trong kernel của `socketpair` và của socket bind vào file `.sock` là một, nên số đo không đổi.
- **Giảm nhiễu**: hai tiến trình được ghim vào CPU 0 và CPU 1 (`sched_setaffinity`); chương trình dịch bằng `-O2`, không sanitizer; nên chạy ba lần và lấy kết quả ổn định.

### Môi trường đo

| Mục | Giá trị (lấy bằng) |
| --- | --- |
| CPU, số core (`lscpu`) |  |
| RAM |  |
| Kernel (`uname -r`) |  |
| Trình biên dịch (`gcc --version`) |  |
| Cấu hình ghim CPU | `-a 0 -b 1` (khác core) và `-a 0 -b 0` (cùng core) |

### Kết quả

Độ trễ một chiều p50 (µs):

| Cơ chế | 64 B | 1 KiB | 4 KiB | 16 KiB | 64 KiB |
| --- | --- | --- | --- | --- | --- |
| pipe |  |  |  |  |  |
| unix\_stream |  |  |  |  |  |
| unix\_seqp |  |  |  |  |  |
| unix\_dgram |  |  |  |  |  |
| tcp |  |  |  |  |  |

Thông lượng một chiều (MB/s):

| Cơ chế | 64 B | 1 KiB | 4 KiB | 16 KiB | 64 KiB |
| --- | --- | --- | --- | --- | --- |
| pipe |  |  |  |  |  |
| unix\_stream |  |  |  |  |  |
| unix\_seqp |  |  |  |  |  |
| unix\_dgram |  |  |  |  |  |
| tcp |  |  |  |  |  |

Chèn hai hình `latency.png` và `throughput.png` do `plot.py` tạo ra từ `results.csv`; cột p99 nằm trong file CSV.

### Hướng phân tích

Các nhận định dưới là kỳ vọng để kiểm chứng bằng số đo của bạn; nếu số đo khác, hãy nêu và giải thích sự khác biệt.

- **TCP loopback so với Unix socket**: TCP thường chậm hơn ở tin nhỏ vì mỗi gói đi qua toàn bộ network stack (header, phân đoạn, theo dõi ACK), còn Unix socket chỉ sao chép dữ liệu giữa các buffer trong kernel.
- **Thông lượng theo kích thước**: với gói nhỏ, thông lượng bị giới hạn bởi số system call mỗi giây; với gói lớn, chi phí cố định mỗi lời gọi được chia cho nhiều byte hơn và đường cong tiến gần tới biên băng thông sao chép bộ nhớ.
- **Stream so với datagram, seqpacket**: `SOCK_DGRAM` và `SOCK_SEQPACKET` giữ ranh giới tin nhắn nên không cần tách frame ở tầng ứng dụng, nhưng kích thước tối đa mỗi gói bị giới hạn bởi bộ đệm gửi của socket.
- **Đuôi phân phối (p99 so với p50)**: khoảng cách lớn do lập lịch và ngắt. So sánh cùng core và khác core: khi ghim cùng core, hai tiến trình phải luân phiên nên mỗi lượt ping-pong có chi phí chuyển ngữ cảnh; khi khác core thì có chi phí đánh thức core kia và chuyển cache.
- **Shared memory** không được đo ở đây. Nó thường nhanh hơn mọi cơ chế trên vì không sao chép dữ liệu qua kernel, nhưng người dùng phải tự đồng bộ bằng semaphore hoặc mutex.

## 6. Hạn chế và hướng phát triển

Hạn chế lớn nhất là server chạy một luồng và chưa có xác thực hay mã hóa. Mỗi hạn chế dưới đây có một hướng khắc phục tương ứng.

| Hạn chế | Hướng phát triển |
| --- | --- |
| Một luồng: một `handle_message` chậm làm chậm tất cả | Nhiều worker, mỗi worker một `epoll` (với TCP có thể dùng `SO_REUSEPORT`), hoặc đẩy việc nặng sang thread pool |
| Không xác thực, không mã hóa, không lưu lịch sử | TLS cho kết nối TCP; giới hạn user Unix bằng `SO_PEERCRED`; lưu lịch sử ra file |
| File socket đặt `chmod 0666`, mọi user trên máy đều kết nối được | Đặt quyền `0660` cho một nhóm riêng và kiểm tra UID khi nhận kết nối |
| Server chỉ lắng nghe IPv4 | Thêm socket `AF_INET6` (dual-stack) |
| Level-triggered, mỗi sự kiện đọc một lượt | Thử edge-triggered (`EPOLLET`), đọc đến `EAGAIN`, đo ảnh hưởng lên thông lượng |
| Benchmark chưa có shared memory và chỉ đo một cặp tiến trình | Thêm shared memory kèm semaphore; đo với nhiều client đồng thời |
| Chưa truyền được file descriptor giữa các tiến trình | Dùng `sendmsg`, `recvmsg` với `SCM_RIGHTS` trên Unix socket |

## 7. Hướng dẫn build, chạy và kịch bản demo

Chỉ cần `make` là build được cả server, client và benchmark trên Arch Linux; phần dưới hướng dẫn từng bước và kịch bản demo khoảng 5 đến 7 phút.

### Yêu cầu và build

```bash
sudo pacman -S base-devel gdb valgrind strace socat tmux python-matplotlib
make          # bản -O2 cho demo và benchmark
make debug    # server và client với ASan, UBSan khi phát triển
```

Benchmark luôn phải build bằng `make` (có `-O2`, không sanitizer), vì sanitizer làm số đo sai lệch.

### Chạy

```bash
./server                       # Unix: /tmp/chat.sock, TCP: cổng 9000
./server /tmp/x.sock 9100      # chỉ định đường dẫn và cổng
./client unix                  # kết nối /tmp/chat.sock
./client tcp 127.0.0.1 9000    # kết nối qua TCP
./bench > results.csv          # khoảng 30 giây
python3 plot.py results.csv    # tạo latency.png và throughput.png
```

Dừng server bằng `Ctrl+C`: server đóng kết nối, giải phóng bộ nhớ và xóa file socket.

### Kịch bản demo

1. **Khởi động**: mở 4 terminal (dùng `tmux`) chạy server, hai client Unix và một client TCP. Log của server hiển thị loại kết nối và danh tính đối phương, ví dụ `unix pid=1157 uid=1000` hoặc `tcp 127.0.0.1:47956`.
2. **Chat**: đổi tên bằng `/nick`, nhắn chung, `/list`, `/msg`. Điểm cần chỉ ra là client Unix và client TCP cùng một phòng.
3. **Chịu lỗi**: `kill -9` một client. Server ghi log đóng kết nối, thông báo cho những người còn lại và không crash.
4. **Quan sát hệ thống**: dùng `ss` để thấy các socket đang lắng nghe và kết nối, `strace` để thấy `epoll_wait`, `accept4`, `read`, `sendto` (là `send`) trong lúc chat.
5. **Kiểm tra giao thức**: gửi văn bản thô thì bị ngắt, gửi đúng frame thì được chấp nhận.
6. **Benchmark**: chiếu hai biểu đồ độ trễ và thông lượng, rút ra kết luận từ số đo của bạn.

Lệnh cho bước 4 và 5:

```bash
ss -xlp | grep chat.sock                      # socket Unix đang lắng nghe
ss -tlnp | grep 9000                          # socket TCP đang lắng nghe
sudo strace -p "$(pgrep -x server)" -e trace=epoll_wait,accept4,read,sendto,epoll_ctl

echo "hello" | socat - UNIX-CONNECT:/tmp/chat.sock                    # văn bản thô: bị ngắt
printf '\x00\x00\x00\x05hello' | socat -t1 - UNIX-CONNECT:/tmp/chat.sock   # frame hợp lệ: chấp nhận
```

### Xử lý sự cố thường gặp

| Triệu chứng | Nguyên nhân và cách xử lý |
| --- | --- |
| `undefined reference to 'main'` khi build | File `.c` rỗng hoặc thiếu hàm `main` (chưa lưu hay chép thiếu); kiểm tra bằng `wc -l` và `grep -n "int main"` |
| `bind(tcp): Address already in use` | Cổng đang bị tiến trình khác dùng; tìm bằng `ss -tlnp`, hoặc chạy `./server /tmp/chat.sock 9100` |
| Client báo `No such file or directory` | Server chưa chạy hoặc khác đường dẫn socket |
| Client báo `Connection refused` | File socket còn sót nhưng không có tiến trình lắng nghe; chạy lại server (server tự `unlink` file cũ) |
| Dùng `nc` hoặc `socat` gõ chữ thô thì bị ngắt | Đúng thiết kế: giao thức yêu cầu tiền tố độ dài 4 byte; dùng `./client` |
| Số benchmark nhảy lung tung | Máy ảo, máy đang tải hoặc chạy bản có sanitizer; build lại bằng `make` và chạy lúc máy rảnh |

## Tài liệu tham khảo

- Linux man-pages: `socket(2)`, `accept4(2)`, `epoll(7)`, `epoll_ctl(2)`, `unix(7)`, `tcp(7)`, `pipe(7)`, `socketpair(2)`, `signal(7)`. [man7.org/linux/man-pages](https://man7.org/linux/man-pages/)
- Brian Hall, *Beej's Guide to Network Programming*. [beej.us/guide/bgnet](https://beej.us/guide/bgnet/)
- Michael Kerrisk, *The Linux Programming Interface*, No Starch Press, 2010: các chương về IPC, socket và mô hình I/O thay thế (`epoll`).
