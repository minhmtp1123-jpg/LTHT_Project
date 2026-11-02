#!/bin/bash
# test.sh - Kich ban kiem thu tich hop tu dong cho Chat Server & Client
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SOCK_PATH="/tmp/test_chat_$$.sock"
TCP_PORT=9876
USERS_DB="/tmp/test_users_$$.db"
CHAT_LOG="/tmp/test_chat_$$.log"
SERVER_BIN="./server"

cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -f "$SOCK_PATH" "$USERS_DB" "$USERS_DB.tmp" "$CHAT_LOG"
}
trap cleanup EXIT INT TERM

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}  BAT DAU KIEM THU TICH HOP CHAT SERVER (SOCKET IPC)  ${NC}"
echo -e "${BLUE}======================================================${NC}"

# 1. Kiem tra binary
if [ ! -x "$SERVER_BIN" ]; then
    echo -e "${RED}[LOI] Chua bien dich server. Chay make truoc!${NC}"
    exit 1
fi

rm -f "$SOCK_PATH" "$USERS_DB" "$CHAT_LOG"

# 2. Khoi dong Server
echo -e "\n${YELLOW}[1/12] Khoi dong Server epoll (Unix Socket + TCP)...${NC}"
"$SERVER_BIN" -u "$SOCK_PATH" -p "$TCP_PORT" -d "$USERS_DB" -l "$CHAT_LOG" &
SERVER_PID=$!
sleep 0.5

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo -e "${RED}[THAT BAI] Server khong khoi dong duoc!${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] Server dang chay voi PID=$SERVER_PID${NC}"

python3 - <<EOF
import socket
import struct
import sys
import time

SOCK_PATH = "$SOCK_PATH"
TCP_PORT = $TCP_PORT

def send_frame(sock, text):
    data = text.encode('utf-8')
    header = struct.pack('>I', len(data))
    sock.sendall(header + data)

def read_frame(sock):
    header = sock.recv(4)
    if not header or len(header) < 4:
        return None
    length = struct.unpack('>I', header)[0]
    data = b""
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            break
        data += chunk
    return data.decode('utf-8')

def read_until(sock, keyword, max_tries=30):
    for _ in range(max_tries):
        f = read_frame(sock)
        if f and keyword in f:
            return f
    return None

def expect_ok(reply):
    assert reply and reply.startswith("+OK"), f"Ky vong +OK nhung nhan: {reply}"

# Test 2: Dang ky qua Unix Socket
print("${YELLOW}[2/12] Test dang ky tai khoan qua Unix Domain Socket...${NC}")
s_unix = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s_unix.connect(SOCK_PATH)
read_frame(s_unix) # Doc greeting
send_frame(s_unix, "/register alice matkhau123")
rep = read_frame(s_unix)
expect_ok(rep)
print("${GREEN}[OK] Dang ky thanh cong alice qua Unix socket${NC}")

# Test 3: Dang ky qua TCP Socket
print("${YELLOW}[3/12] Test dang ky tai khoan qua TCP Socket...${NC}")
s_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s_tcp.connect(("127.0.0.1", TCP_PORT))
read_frame(s_tcp) # Greeting
send_frame(s_tcp, "/register bob matkhau456")
rep = read_frame(s_tcp)
expect_ok(rep)
print("${GREEN}[OK] Dang ky thanh cong bob qua TCP socket${NC}")

# Test 4: Chong dang ky trung username
print("${YELLOW}[4/12] Test ngan chan dang ky trung ten...${NC}")
s_dup = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s_dup.connect(SOCK_PATH)
read_frame(s_dup)
send_frame(s_dup, "/register alice matkhau_khac")
rep = read_frame(s_dup)
assert rep and rep.startswith("-ERR"), f"Ky vong -ERR nhung nhan: {rep}"
s_dup.close()
print("${GREEN}[OK] Server tu choi dang ky trung ten dung thiet ke${NC}")

# Test 5: Dang nhap sai mat khau va ngat sau 3 lan
print("${YELLOW}[5/12] Test khoa ket noi khi nhap sai mat khau 3 lan...${NC}")
s_fail = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s_fail.connect(("127.0.0.1", TCP_PORT))
read_frame(s_fail)
for i in range(3):
    send_frame(s_fail, "/login alice saimatkhau")
    rep = read_frame(s_fail)
# Lan thu 3 server phai ngat ket noi
rep_after = read_frame(s_fail)
assert rep_after is None, f"Ky vong socket dong nhung nhan: {rep_after}"
s_fail.close()
print("${GREEN}[OK] Server ngat ket noi sau 3 lan sai mat khau${NC}")

# Test 6: Chat broadcast cheo giua Unix Socket va TCP Socket
print("${YELLOW}[6/12] Test chat broadcast cheo giua Unix Socket va TCP...${NC}")
send_frame(s_unix, "Xin chao tu Alice qua Unix socket!")
msg_bob = read_until(s_tcp, "[alice] Xin chao tu Alice qua Unix socket!")
assert msg_bob is not None, "Bob khong nhan duoc tin broadcast cua Alice"
print("${GREEN}[OK] Broadcast xuyen suot giua Unix Socket va TCP thanh cong${NC}")

# Test 7: Tin nhan rieng tu /msg
print("${YELLOW}[7/12] Test tin nhan rieng tu /msg...${NC}")
send_frame(s_unix, "/msg bob thong diep bi mat")
msg_priv = read_until(s_tcp, "[rieng tu tu alice] thong diep bi mat")
assert msg_priv is not None, "Bob khong nhan duoc tin nhan rieng tu cua Alice"
print("${GREEN}[OK] Tin nhan rieng tu hoat dong chinh xac${NC}")

# Test 8: Lenh /whoami va /stats
print("${YELLOW}[8/12] Test lenh /whoami va /stats...${NC}")
send_frame(s_unix, "/whoami")
rep_who = read_until(s_unix, "alice")
assert rep_who is not None, "/whoami loi khong tra ve username"

send_frame(s_unix, "/stats")
rep_stats = read_until(s_unix, "Thong ke")
assert rep_stats is not None, "/stats loi khong tra ve thong ke"
print("${GREEN}[OK] Cac lenh /whoami va /stats phan hoi day du thong tin${NC}")

# Test 9: Doi mat khau qua /passwd
print("${YELLOW}[9/12] Test doi mat khau qua /passwd...${NC}")
send_frame(s_unix, "/passwd matkhau123 matkhaumoi789")
rep_pwd = read_until(s_unix, "+OK")
assert rep_pwd is not None, f"Doi mat khau that bai: {rep_pwd}"
s_unix.close()

# Thu dang nhap lai bang mat khau moi
s_re = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s_re.connect(SOCK_PATH)
read_frame(s_re)
send_frame(s_re, "/login alice matkhaumoi789")
rep_login = read_frame(s_re)
expect_ok(rep_login)
s_re.close()
s_tcp.close()
print("${GREEN}[OK] Doi mat khau thanh cong va dang nhap lai voi mat khau moi${NC}")

# Test 10: Framing Security - Frame co do dai > 4096 bi ngat
print("${YELLOW}[10/12] Test phong thu Framing: cat ket noi khi payload vuot nguong...${NC}")
s_bad = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s_bad.connect(SOCK_PATH)
read_frame(s_bad)
# Gui do dai fake 50000 byte
s_bad.sendall(struct.pack('>I', 50000) + b'X' * 64)
time.sleep(0.1)
res = s_bad.recv(10)
assert len(res) == 0, "Server khong ngat client gui frame gia mao do dai"
s_bad.close()
print("${GREEN}[OK] Server ngat ket noi ngay lap tuc khi gap frame sai do dai${NC}")

# Test 11: Fault tolerance - Ngat client dot ngot
print("${YELLOW}[11/12] Test kha nang chiu loi khi client dong dot ngot...${NC}")
s_alive = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s_alive.connect(("127.0.0.1", TCP_PORT))
read_frame(s_alive)
send_frame(s_alive, "/login bob matkhau456")
read_frame(s_alive)

s_crash = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s_crash.connect(("127.0.0.1", TCP_PORT))
read_frame(s_crash)
send_frame(s_crash, "/login alice matkhaumoi789")
read_frame(s_crash)
s_crash.close() # Dong dot ngot

found_leave = read_until(s_alive, "alice da roi phong")
assert found_leave is not None, "Server khong phat hien hoac khong broadcast khi client crash"
s_alive.close()
print("${GREEN}[OK] Server chiu loi tot va phat hien client dong ket noi${NC}")

EOF

# Test 12: Graceful shutdown
echo -e "${YELLOW}[12/12] Test tat server an toan bang SIGINT/SIGTERM...${NC}"
kill -TERM "$SERVER_PID"
wait "$SERVER_PID" || true
if [ -e "$SOCK_PATH" ]; then
    echo -e "${RED}[THAT BAI] File socket van con ton tai sau khi server tat!${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] Server da tat sach se va unlink file socket${NC}"

echo -e "\n${GREEN}======================================================${NC}"
echo -e "${GREEN}   TAT CA 12/12 KIEM THU TICH HOP DA PASS THANH CONG! ${NC}"
echo -e "${GREEN}======================================================${NC}"
