# Phân tích dự án: Chat server đa client và IPC bằng socket

## 1. Tổng quan

Đây là đồ án lập trình hệ thống trên Linux, viết bằng ngôn ngữ **C**. Dự án gồm hai phần chính:

1. **Chat server đa client**: Đơn tiến trình, đơn luồng, phục vụ đồng thời các client kết nối qua Unix domain socket (`AF_UNIX`) và TCP (`AF_INET`). Server tích hợp xác thực tài khoản qua `users.db` (băm mật khẩu với yescrypt) và lưu trữ nhật ký trò chuyện qua `chat.log` (hỗ trợ phát lại lịch sử hội thoại gần nhất).
2. **Chương trình benchmark**: Đo lường và so sánh định lượng độ trễ (latency p50/p99) và thông lượng (throughput MB/s) của 5 cơ chế IPC cơ bản trên Linux giữa hai tiến trình.

Server dùng `epoll` và socket non-blocking để quản lý kết nối mà không cần tạo một luồng riêng cho từng client. Client dùng `poll()` để chờ đồng thời dữ liệu từ bàn phím (`STDIN_FILENO`) và dữ liệu từ socket server.

## 2. Công nghệ và kiến thức chính

| Công nghệ / khái niệm | Vai trò trong dự án |
|---|---|
| C và API POSIX/Linux | Quản lý bộ nhớ, tiến trình, file descriptor, socket, tín hiệu và thời gian |
| Socket API | Tạo kết nối hai chiều giữa client và server |
| Unix domain socket (`AF_UNIX`) | Giao tiếp giữa các tiến trình trên cùng máy thông qua đường dẫn socket, mặc định `/tmp/chat.sock` |
| TCP (`AF_INET`, `SOCK_STREAM`) | Giao tiếp qua network stack; server lắng nghe cổng TCP, mặc định `9000` |
| `epoll` | Multiplex I/O phía server: chờ sự kiện trên cả hai listener và mọi client trong một event loop duy nhất |
| `poll()` | Multiplex I/O phía client: chờ đồng thời stdin và socket |
| Non-blocking I/O | Ngăn một socket chậm làm dừng vòng lặp server; các thao tác có thể tiếp tục khi fd sẵn sàng |
| Length-Prefix Framing | Tạo ranh giới thông điệp trên các kết nối kiểu byte stream |
| Xác thực & libxcrypt | Quản lý tài khoản trong `users.db`, băm mật khẩu bằng yescrypt (có salt ngẫu nhiên), chống timing attack |
| Append-only logging | Ghi nhật ký tin nhắn và sự kiện vào `chat.log`, phát lại 20 dòng gần nhất bằng `pread` |
| IPC và `fork()` | Benchmark các kênh trao đổi dữ liệu giữa tiến trình cha và con |
| `clock_gettime(CLOCK_MONOTONIC)` | Đo khoảng thời gian ổn định độ chính xác microsecond cho benchmark |
| GCC, Make, Sanitizers | Build các chương trình; ASan và UBSan hỗ trợ phát hiện lỗi bộ nhớ và hành vi không xác định |
| Python, CSV, Matplotlib | Đọc số liệu benchmark và tạo biểu đồ độ trễ, thông lượng |

### Vì sao cần framing?

TCP và Unix `SOCK_STREAM` truyền một dòng byte, không bảo đảm một lần `send()` ở phía gửi tương ứng với một lần `read()` ở phía nhận. Một thông điệp có thể bị chia qua nhiều lần đọc; nhiều thông điệp cũng có thể được nhận cùng lúc. Vì thế ứng dụng tự quy ước mỗi thông điệp thành:

```text
[4 byte độ dài big-endian][payload]
```

Độ dài là `uint32_t`, chuyển đổi qua `htonl()`/`ntohl()`. Payload là văn bản, từ 1 đến 4096 byte, không có byte kết thúc NUL. Server kiểm tra độ dài trước khi xử lý; frame có độ dài 0 hoặc lớn hơn giới hạn sẽ làm kết nối bị đóng ngay lập tức.

## 3. Kiến trúc và luồng hoạt động

### 3.1 Server

Server tạo hai listener: Unix domain socket và TCP IPv4. Cả hai được đăng ký vào cùng một đối tượng `epoll`; mỗi kết nối mới được chấp nhận bằng `accept4()` với `SOCK_NONBLOCK | SOCK_CLOEXEC`, rồi cũng được thêm vào epoll.

Mỗi client có cấu trúc trạng thái riêng gồm tên, loại kết nối, trạng thái xác thực (`authed`), buffer đọc, buffer ghi và liên kết vào danh sách client. Event loop phân loại sự kiện bằng `kind` trong `struct handle`, được lưu qua `epoll_event.data.ptr`.

Luồng xử lý điển hình:

1. `epoll_wait()` trả về listener hoặc client có sự kiện.
2. Với listener, server lặp `accept4()` đến `EAGAIN`, đăng ký client mới và gửi thông báo yêu cầu đăng nhập.
3. Với client có `EPOLLIN`, server đọc byte vào `rbuf`, tách tất cả frame đầy đủ, rồi chuyển payload cho bộ xử lý.
4. Nếu client chưa đăng nhập, chỉ chấp nhận `/login` hoặc `/register`. Nếu đã đăng nhập, tin nhắn thường được ghi vào `chat.log` và phát tới các client đang kết nối; lệnh có thể tạo phản hồi riêng hoặc gửi tin nhắn riêng.
5. Phản hồi được đưa vào hàng đợi `wbuf`. Server thử gửi ngay; nếu socket tạm thời không ghi được (`EAGAIN`/`EWOULDBLOCK`), phần còn lại được giữ lại và `EPOLLOUT` được bật.
6. Khi có `EPOLLOUT`, `flush_client()` gửi phần còn lại. Khi hàng đợi rỗng, server tắt theo dõi `EPOLLOUT`.
7. Client lỗi, ngắt kết nối hoặc gửi `/quit` được đánh dấu đã chết (`dead = 1`). Bộ nhớ chỉ được giải phóng tại `sweep_dead()` sau khi xử lý xong batch sự kiện hiện tại, tránh giải phóng sớm đối tượng mà event hoặc vòng broadcast còn tham chiếu.

Server dùng chế độ epoll level-triggered. Mỗi sự kiện đọc một lần; nếu vẫn còn dữ liệu, fd tiếp tục được báo sẵn sàng ở vòng sau. Buffer ghi bị giới hạn 1 MiB để ngắt client đọc quá chậm thay vì cho hàng đợi tăng không giới hạn.

### 3.2 Client

Client có thể kết nối theo một trong hai cách:

- `unix`: `AF_UNIX`, mặc định `/tmp/chat.sock`.
- `tcp`: dùng `getaddrinfo()` với host/cổng, mặc định `127.0.0.1:9000`.

Sau khi kết nối, client thực hiện thủ tục xác thực (hỏi tên người dùng và mật khẩu, ẩn ký tự khi gõ, gửi lệnh `/login` hoặc `/register` và chờ phản hồi `+OK`). Sau khi xác thực thành công, client dùng `poll()` trên stdin và socket. Dữ liệu stdin được đọc bằng `read()` và tách theo dòng; mỗi dòng được đóng khung rồi gửi. Dữ liệu từ server được tích lũy, phân tích header độ dài và in payload khi đã nhận đủ frame. Nếu stdin kết thúc, client gửi phần văn bản còn lại (nếu có), gọi `shutdown(..., SHUT_WR)` và tiếp tục nhận cho đến khi server đóng kết nối.

### 3.3 Các lệnh trong giao thức

**Giai đoạn trước khi đăng nhập:**

| Lệnh | Cú pháp | Hành vi |
|---|---|---|
| Đăng nhập | `/login <tên> <mật khẩu>` | Kiểm tra tài khoản trong `users.db`. Trả về `+OK <tên>` hoặc `-ERR <lý do>` |
| Đăng ký | `/register <tên> <mật khẩu>` | Tạo tài khoản mới trong `users.db` và đăng nhập ngay |

**Giai đoạn sau khi đăng nhập:**

| Lệnh / nội dung | Hành vi |
|---|---|
| Văn bản thường | Broadcast tới các client đang kết nối và ghi vào `chat.log` |
| `/list` | Liệt kê client đang online và loại kết nối (`unix` hoặc `tcp`) |
| `/msg <tên> <nội dung>` | Gửi tin nhắn riêng (không ghi vào `chat.log`) |
| `/help` | Hiện trợ giúp |
| `/quit` | Gửi lời chào tạm biệt và rời phòng |

Tên hiển thị được cố định theo tên đăng nhập (chỉ gồm chữ, số, `_`, tối đa 16 ký tự).

## 4. Benchmark IPC

`bench.c` tạo hai tiến trình bằng `fork()`, sau đó đo năm cơ chế:

| Tên trong kết quả | Cơ chế/kênh |
|---|---|
| `pipe` | Hai pipe một chiều để tạo giao tiếp hai chiều |
| `unix_stream` | `socketpair(AF_UNIX, SOCK_STREAM)` |
| `unix_dgram` | `socketpair(AF_UNIX, SOCK_DGRAM)` |
| `unix_seqp` | `socketpair(AF_UNIX, SOCK_SEQPACKET)` |
| `tcp` | TCP loopback trên `127.0.0.1`, cổng được hệ điều hành cấp động |
| `shm` | POSIX Shared Memory (`shm_open` + `mmap` + POSIX Semaphore) |

Kích thước thông điệp được thử là 64 B, 1 KiB, 4 KiB, 16 KiB và 64 KiB.

- **Độ trễ:** ping-pong; tiến trình cha gửi một thông điệp và con echo lại. Thời gian đo là RTT bằng đồng hồ monotonic, bỏ các vòng warm-up, rồi báo cáo RTT/2 dưới dạng trung bình, p50 và p99.
- **Thông lượng:** cha gửi liên tục một chiều; con đọc đủ dữ liệu rồi gửi một byte ACK. Kết quả tính theo MB/s thập phân (`1 MB = 10^6 byte`).
- Hai tiến trình mặc định được ghim lần lượt vào CPU 0 và CPU 1. Có thể thử cùng CPU, CPU khác hoặc tắt ghim để thấy ảnh hưởng của lập lịch và chuyển ngữ cảnh.
- `plot.py` đọc CSV và tạo `latency.png` cùng `throughput.png`.

## 5. Build và chạy

Các lệnh dưới đây chạy từ thư mục `src/`:

```bash
make                 # build server, client và bench với -O2
make debug           # build server/client với AddressSanitizer và UBSan
make test            # chạy bộ kiểm thử tích hợp tự động 12 kịch bản
./server             # Unix /tmp/chat.sock và TCP cổng 9000
./client -r unix     # đăng ký tài khoản mới qua Unix socket
./client unix        # đăng nhập qua Unix socket
./client tcp         # đăng nhập TCP tới 127.0.0.1:9000
./bench > results.csv
python3 plot.py results.csv
make clean
```

`plot.py` cần Python 3 và Matplotlib. Không nên dùng bản sanitizer để benchmark vì instrumentation làm thay đổi thời gian chạy.

Server nhận các tham số tùy chọn:
```bash
./server [-u unix_path] [-p port] [-d users.db] [-l chat.log]
```

## 6. Cấu trúc mã nguồn

| Tệp | Trách nhiệm |
|---|---|
| [`src/server.c`](../src/server.c) | Listener Unix/TCP, event loop epoll, xác thực tài khoản, ghi lịch sử, framing và hàng đợi ghi |
| [`src/client.c`](../src/client.c) | Kết nối Unix/TCP, xác thực an toàn (ẩn mật khẩu), poll stdin/socket, framing và giao diện dòng lệnh |
| [`src/bench.c`](../src/bench.c) | Dựng các kênh IPC, fork tiến trình đo, tính thống kê độ trễ/thông lượng và xuất CSV |
| [`src/plot.py`](../src/plot.py) | Đọc CSV và vẽ biểu đồ phân phối độ trễ và thông lượng |
| [`src/test.sh`](../src/test.sh) | Kịch bản kiểm thử tích hợp tự động toàn bộ tính năng và edge cases |
| [`src/Makefile`](../src/Makefile) | Các target build, debug, benchmark, plot và clean |
| [`README.md`](../README.md) | Hướng dẫn tổng quan, demo, benchmark và ghi chú thiết kế |
| [`docs/BaoCao.md`](./BaoCao.md) | Báo cáo đồ án chi tiết và phân tích số liệu đo lường |
| [`users.db`](../src/users.db) | Cơ sở dữ liệu tài khoản (chỉ lưu hash mật khẩu yescrypt, quyền `0600`) |
| [`chat.log`](../src/chat.log) | Nhật ký tin nhắn công khai và sự kiện phòng chat (quyền `0600`) |

## 7. Kiến thức hệ thống thể hiện trong mã

- **File descriptor và vòng đời socket:** `socket()`, `bind()`, `listen()`, `accept4()`, `connect()`, `shutdown()` và `close()`.
- **Địa chỉ mạng và byte order:** `sockaddr_un`, `sockaddr_in`, `htonl()`/`ntohl()`, `htons()`/`ntohs()`, `getaddrinfo()` và `inet_ntop()`.
- **Multiplex I/O:** khác biệt vai trò giữa `epoll` phía server và `poll()` phía client; đăng ký, sửa đổi và gỡ fd khỏi event loop.
- **I/O không đồng bộ ở mức ứng dụng:** xử lý short read/write, `EINTR`, `EAGAIN` và `EWOULDBLOCK`; giữ phần dữ liệu chưa gửi trong hàng đợi.
- **Quản lý bộ nhớ và an toàn:** buffer cố định cho đọc, buffer ghi tăng động, xóa dấu vết mật khẩu bằng `explicit_bzero`, trì hoãn giải phóng client đã đóng để tránh Use-After-Free.
- **An toàn khi ghi socket:** `MSG_NOSIGNAL` và bỏ qua `SIGPIPE`; xử lý peer đóng kết nối.
- **Tín hiệu và dọn dẹp:** `SIGINT`/`SIGTERM` đặt cờ dừng, event loop thoát rồi đóng fd và xóa file socket Unix.
- **Quản trị kết nối:** `SO_REUSEADDR`, `TCP_NODELAY`, `SO_PEERCRED` và giới hạn kích thước hàng đợi.
- **Đo hiệu năng:** warm-up, percentile, affinity, so sánh cùng core/khác core, CSV và biểu đồ.

## 8. Giới hạn và hướng phát triển

- Server hiện là đơn luồng; phép băm mật khẩu `crypt_r` với yescrypt tốn 10–15 ms CPU chạy trực tiếp trong event loop, có thể làm trễ tạm thời các kết nối khác. Hướng mở rộng là dùng thread pool cho tác vụ băm.
- Server bind TCP bằng `AF_INET` và `INADDR_ANY`, tức chỉ tạo listener IPv4. Có thể nâng cấp lên socket `AF_INET6` dual-stack.
- Chưa có mã hóa kênh truyền (TLS) cho TCP; hướng phát triển là tích hợp OpenSSL.
- Chưa có lệnh đổi mật khẩu `/passwd` trực tiếp từ client.
- Benchmark hiện đo 5 cơ chế (pipe, unix stream, unix datagram, unix seqpacket, tcp); hướng phát triển là bổ sung Shared Memory kèm POSIX Semaphore.

## 9. Tham khảo nhanh theo tài liệu trong repository

- [README chính](../README.md): cách chạy demo, giải thích event loop, giao thức, các ghi chú thiết kế và xử lý sự cố.
- [Báo cáo đồ án](./BaoCao.md): nền tảng IPC, mô tả cài đặt, phương pháp benchmark và phân tích kết quả đo.
