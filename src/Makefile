CC      ?= gcc
WARN    := -Wall -Wextra
OPT     := -O2 -g
SAN     := -fsanitize=address,undefined

.PHONY: all debug bench-run plot clean

# Ban phat hanh (dung -O2, khong sanitizer). Benchmark LUON build kieu nay.
all: server client bench

server: server.c
	$(CC) $(WARN) $(OPT) -o $@ $< -lcrypt

client: client.c
	$(CC) $(WARN) $(OPT) -o $@ $<

bench: bench.c
	$(CC) $(WARN) $(OPT) -o $@ $<

# Ban debug cho server/client (ASan + UBSan) de phat hien loi bo nho
debug:
	$(CC) $(WARN) -g $(SAN) -o server server.c -lcrypt
	$(CC) $(WARN) -g $(SAN) -o client client.c

bench-run: bench
	./bench > results.csv

plot: results.csv
	python3 plot.py results.csv

clean:
	rm -f server client bench results.csv latency.png throughput.png