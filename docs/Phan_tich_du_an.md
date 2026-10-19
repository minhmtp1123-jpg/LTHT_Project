# Phân tích dự án: Chat server đa client và IPC bằng socket

## 1. Tổng quan

Đây là đồ án lập trình hệ thống trên Linux, viết chủ yếu bằng C. Dự án gồm hai phần:

1. Một chat server đơn tiến trình, đơn luồng, cho phép nhiều client giao tiếp trong cùng phòng qua Unix domain socket hoặc TCP.
2. Một chương trình benchmark đo độ trễ và thông lượng của một số cơ chế IPC giữa hai tiến trình.

Server dùng `epoll` và socket non-blocking để quản lý kết nối mà không cần tạo một luồng riêng cho từng client. Client dùng `poll()` để chờ đồng thời dữ liệu từ bàn phím và dữ liệu từ server.

## 2. Công nghệ và kiến thức chính

| Công nghệ / khái niệm | Vai trò trong dự án |
|---|---|
| C và API POSIX/Linux | Quản lý bộ nhớ, tiến trình, file descriptor, socket, tín hiệu và thời gian |
| Socket API | Tạo kết nối hai chiều giữa client và server |
| Unix domain socket (`AF_UNIX`) | Giao tiếp giữa các tiến trình trên cùng máy thông qua đường dẫn socket, mặc định `/tmp/chat.sock` |
| TCP (`AF_INET`, `SOCK_STREAM`) | Giao tiếp qua network stack; server lắng nghe cổng TCP, mặc định `9000` |
| `epoll` | Multiplex I/O phía server: chờ sự kiện trên listener và mọi client trong một event loop |
| `poll()` | Multiplex I/O phía client: chờ đồng thời stdin và socket |
| Non-blocking I/O | Ngăn một socket chậm làm dừng vòng lặp server; các thao tác có thể tiếp tục khi fd sẵn sàng |
| Framing | Tạo ranh giới thông điệp trên các kết nối kiểu byte stream |
| IPC và `fork()` | Benchmark các kênh trao đổi dữ liệu giữa tiến trình cha và con |
| `clock_gettime(CLOCK_MONOTONIC)` | Đo khoảng thời gian ổn định cho benchmark |
| GCC, Make, sanitizer | Build các chương trình; ASan và UBSan hỗ trợ phát hiện lỗi bộ nhớ và hành vi không xác định |
| Python, CSV, Matplotlib | Đọc số liệu benchmark và tạo biểu đồ độ trễ, thông lượng |

### Vì sao cần framing?

TCP và Unix `SOCK_STREAM` truyền một dòng byte, không bảo đảm một lần `send()` ở phía gửi tương ứng với một lần `read()` ở phía nhận. Một thông điệp có thể bị chia qua nhiều lần đọc; nhiều thông điệp cũng có thể được nhận cùng lúc. Vì thế ứng dụng tự quy ước mỗi thông điệp thành:

```text
[4 byte độ dài big-endian][payload]
```

Độ dài là `uint32_t`, chuyển đổi qua `htonl()`/`ntohl()`. Payload là văn bản, từ 1 đến 4096 byte, không có byte kết thúc NUL. Server kiểm tra độ dài trước khi xử lý; frame có độ dài 0 hoặc lớn hơn giới hạn sẽ làm kết nối bị đóng.

## 3. Kiến trúc và luồng hoạt động

### 3.1 Server

Server tạo hai listener: Unix domain socket và TCP IPv4. Cả hai được đăng ký vào cùng một đối tượng `epoll`; mỗi kết nối mới được chấp nhận bằng `accept4()` với `SOCK_NONBLOCK | SOCK_CLOEXEC`, rồi cũng được thêm vào epoll.

Mỗi client có cấu trúc trạng thái riêng gồm tên, loại kết nối, buffer đọc, buffer ghi và liên kết vào danh sách client. Event loop phân loại sự kiện bằng `kind` trong `struct handle`, được lưu qua `epoll_event.data.ptr`.

Luồng xử lý điển hình:

1. `epoll_wait()` trả về listener hoặc client có sự kiện.
2. Với listener, server lặp `accept4()` đến `EAGAIN`, đăng ký client mới và gửi lời chào.
3. Với client có `EPOLLIN`, server đọc byte vào `rbuf`, tách tất cả frame đầy đủ, rồi chuyển payload cho bộ xử lý lệnh/chat.
4. Tin nhắn thường được phát tới các client đang kết nối; lệnh có thể tạo phản hồi riêng hoặc gửi tin nhắn riêng.
5. Phản hồi được đưa vào hàng đợi `wbuf`. Server thử gửi ngay; nếu socket tạm thời không ghi được (`EAGAIN`/`EWOULDBLOCK`), phần còn lại được giữ lại và `EPOLLOUT` được bật.
6. Khi có `EPOLLOUT`, `flush_client()` gửi phần còn lại. Khi hàng đợi rỗng, server tắt theo dõi `EPOLLOUT`.
7. Client lỗi, ngắt kết nối hoặc gửi `/quit` được đánh dấu đã chết. Bộ nhớ chỉ được giải phóng tại `sweep_dead()` sau khi xử lý xong batch sự kiện hiện tại, tránh giải phóng sớm đối tượng mà event hoặc vòng broadcast còn tham chiếu.

Server dùng chế độ epoll level-triggered. Mỗi sự kiện đọc một lần; nếu vẫn còn dữ liệu, fd tiếp tục được báo sẵn sàng ở vòng sau. Buffer ghi bị giới hạn 1 MiB để ngắt client đọc quá chậm thay vì cho hàng đợi tăng không giới hạn.

### 3.2 Client

Client có thể kết nối theo một trong hai cách:

- `unix`: `AF_UNIX`, mặc định `/tmp/chat.sock`.
- `tcp`: dùng `getaddrinfo()` với host/cổng, mặc định `127.0.0.1:9000`.

Sau khi kết nối, client dùng `poll()` trên stdin và socket. Dữ liệu stdin được đọc bằng `read()` và tách theo dòng; mỗi dòng được đóng khung rồi gửi. Dữ liệu từ server được tích lũy, phân tích header độ dài và in payload khi đã nhận đủ frame. Nếu stdin kết thúc, client gửi phần văn bản còn lại (nếu có), gọi `shutdown(..., SHUT_WR)` và tiếp tục nhận cho đến khi server đóng kết nối.

### 3.3 Các lệnh chat

| Lệnh / nội dung | Hành vi |
|---|---|
| Văn bản thường | Broadcast tới các client đang kết nối, bao gồm người gửi |
| `/nick <tên>` | Đổi biệt danh; chỉ chấp nhận chữ, số, `_`, tối đa 16 ký tự và không trùng |
| `/list` | Liệt kê client đang online và loại kết nối |
| `/msg <tên> <nội dung>` | Gửi tin nhắn riêng |
| `/help` | Hiện trợ giúp |
| `/quit` | Gửi lời chào tạm biệt và rời phòng |

Tên ban đầu được tạo từ file descriptor, theo dạng `user<fd>`.

## 4. Benchmark IPC

`bench.c` tạo hai tiến trình bằng `fork()`, sau đó đo năm cơ chế:

| Tên trong kết quả | Cơ chế/kênh |
|---|---|
| `pipe` | Hai pipe một chiều để tạo giao tiếp hai chiều |
| `unix_stream` | `socketpair(AF_UNIX, SOCK_STREAM)` |
| `unix_dgram` | `socketpair(AF_UNIX, SOCK_DGRAM)` |
| `unix_seqp` | `socketpair(AF_UNIX, SOCK_SEQPACKET)` |
| `tcp` | TCP loopback trên `127.0.0.1`, cổng được hệ điều hành cấp động |

Đây không phải phép đo cho UDP: `unix_dgram` là Unix domain datagram. `unix_stream`, pipe và TCP là byte stream; `unix_dgram` và `unix_seqp` giữ ranh giới thông điệp.

Kích thước thông điệp được thử là 64 B, 1 KiB, 4 KiB, 16 KiB và 64 KiB.

- **Độ trễ:** ping-pong; tiến trình cha gửi một thông điệp và con echo lại. Thời gian đo là RTT bằng đồng hồ monotonic, bỏ các vòng warm-up, rồi báo cáo RTT/2 dưới dạng trung bình, p50 và p99.
- **Thông lượng:** cha gửi liên tục một chiều; con đọc đủ dữ liệu rồi gửi một byte ACK. Kết quả tính theo MB/s thập phân (`1 MB = 10^6 byte`).
- Hai tiến trình mặc định được ghim lần lượt vào CPU 0 và CPU 1. Có thể thử cùng CPU, CPU khác hoặc tắt ghim để thấy ảnh hưởng của lập lịch và chuyển ngữ cảnh.
- `plot.py` đọc CSV và tạo `latency.png` cùng `throughput.png`.

Số benchmark phụ thuộc mạnh vào CPU, kernel, tải hệ thống, ảo hóa và cấu hình affinity. Nên build không sanitizer, chạy nhiều lần trong điều kiện tương tự và phân tích số đo thực tế thay vì coi kết quả kỳ vọng là kết luận.

## 5. Build và chạy

Các lệnh dưới đây chạy từ thư mục `src/`:

```bash
make                 # build server, client và bench với -O2
make debug           # build server/client với AddressSanitizer và UBSan
./server             # Unix /tmp/chat.sock và TCP cổng 9000
./client unix        # kết nối qua Unix socket
./client tcp         # kết nối TCP tới 127.0.0.1:9000
./bench > results.csv
python3 plot.py results.csv
make clean
```

`plot.py` cần Python 3 và Matplotlib. Không nên dùng bản sanitizer để benchmark vì instrumentation làm thay đổi thời gian chạy.

Server nhận tùy chọn `[unix_path] [tcp_port]`, ví dụ:

```bash
./server /tmp/chat-demo.sock 9100
./client unix /tmp/chat-demo.sock
./client tcp 127.0.0.1 9100
```

`make debug` chỉ build server và client, không build benchmark. Makefile cũng build từng file chương trình riêng; `proto.c` và `util.c` hiện không được tham chiếu trong các target này.

## 6. Cấu trúc mã nguồn

| Tệp | Trách nhiệm |
|---|---|
| [`src/server.c`](../src/server.c) | Listener Unix/TCP, event loop epoll, trạng thái client, framing, lệnh chat và hàng đợi ghi |
| [`src/client.c`](../src/client.c) | Kết nối Unix/TCP, poll stdin/socket, framing và giao diện dòng lệnh |
| [`src/bench.c`](../src/bench.c) | Dựng các kênh IPC, fork tiến trình đo, tính thống kê độ trễ/thông lượng và xuất CSV |
| [`src/plot.py`](../src/plot.py) | Đọc CSV và vẽ biểu đồ |
| [`src/Makefile`](../src/Makefile) | Các target build, debug, benchmark, plot và clean |
| [`src/proto.c`](../src/proto.c) | Chứa helper đọc/ghi đủ byte và gửi frame; chưa được Makefile dùng để build server/client |
| [`src/util.c`](../src/util.c) | Tệp tiện ích hiện không được các target build tham chiếu |
| [`README.md`](../README.md) | Hướng dẫn tổng quan, demo, benchmark và ghi chú thiết kế |
| [`docs/BaoCao.md`](./BaoCao.md) | Báo cáo đồ án và phần trình bày phương pháp đo |

## 7. Kiến thức hệ thống thể hiện trong mã

- **File descriptor và vòng đời socket:** `socket()`, `bind()`, `listen()`, `accept4()`, `connect()`, `shutdown()` và `close()`.
- **Địa chỉ mạng và byte order:** `sockaddr_un`, `sockaddr_in`, `htonl()`/`ntohl()`, `htons()`/`ntohs()`, `getaddrinfo()` và `inet_ntop()`.
- **Multiplex I/O:** khác biệt vai trò giữa `epoll` phía server và `poll()` phía client; đăng ký, sửa đổi và gỡ fd khỏi event loop.
- **I/O không đồng bộ ở mức ứng dụng:** xử lý short read/write, `EINTR`, `EAGAIN` và `EWOULDBLOCK`; giữ phần dữ liệu chưa gửi trong hàng đợi.
- **Quản lý bộ nhớ và trạng thái:** buffer cố định cho đọc, buffer ghi tăng động, danh sách liên kết và trì hoãn giải phóng client đã đóng.
- **An toàn khi ghi socket:** `MSG_NOSIGNAL` và bỏ qua `SIGPIPE`; xử lý peer đóng kết nối.
- **Tín hiệu và dọn dẹp:** `SIGINT`/`SIGTERM` đặt cờ dừng, event loop thoát rồi đóng fd và xóa file socket Unix.
- **Quản trị kết nối:** `SO_REUSEADDR`, `TCP_NODELAY`, `SO_PEERCRED` và giới hạn kích thước hàng đợi.
- **Đo hiệu năng:** warm-up, percentile, affinity, so sánh cùng core/khác core, CSV và biểu đồ.

## 8. Giới hạn và lưu ý khi đánh giá

- Server hiện là đơn luồng; xử lý đồng bộ lâu sẽ ảnh hưởng mọi client.
- Server bind TCP bằng `AF_INET` và `INADDR_ANY`, tức chỉ tạo listener IPv4. Client dùng `getaddrinfo()` nhưng điều đó không làm server tự hỗ trợ IPv6.
- Unix socket được `chmod 0666`; mọi người dùng trên máy có thể thử kết nối. Không nên giữ quyền này trong môi trường cần kiểm soát truy cập.
- Chưa có xác thực người dùng, TLS/mã hóa, lưu lịch sử hoặc giới hạn tốc độ gửi.
- Một client chiếm nhiều hơn một frame payload trong một lần nhận vẫn được xử lý theo buffer và vòng tách frame; tuy nhiên giao thức giới hạn payload từng frame ở 4096 byte.
- Benchmark chỉ đo giao tiếp giữa một cặp tiến trình và chưa bao gồm shared memory. Nó đo các primitive IPC, không đo hiệu năng toàn bộ chat server dưới tải nhiều client.
- Kết quả đo có thể sai lệch bởi việc ghim CPU không thành công, CPU không có số thứ tự như mặc định, máy ảo hoặc tải nền; cần đọc cảnh báo chương trình và ghi rõ môi trường.

Để mở rộng dự án theo hướng sản phẩm, các bước đáng cân nhắc là giới hạn quyền truy cập socket Unix, bổ sung xác thực/ mã hóa cho TCP, hỗ trợ IPv6, giới hạn tốc độ và kiểm thử tải với nhiều client.

## 9. Tham khảo nhanh theo tài liệu trong repository

- [README chính](../README.md): cách chạy demo, giải thích event loop, giao thức, các ghi chú thiết kế và xử lý sự cố.
- [Báo cáo đồ án](./BaoCao.md): nền tảng IPC, mô tả cài đặt, phương pháp benchmark và các hướng phát triển.
- [README trong `src/`](../src/ReadME.md): hướng dẫn build và sử dụng các chương trình trong thư mục mã nguồn.
