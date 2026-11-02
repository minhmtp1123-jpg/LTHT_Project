CC      ?= gcc
WARN    := -Wall -Wextra
OPT     := -O2 -g
SAN     := -fsanitize=address,undefined

.PHONY: all debug bench-run plot clean clean-bench distclean

# Ban phat hanh (dung -O2, khong sanitizer). Benchmark LUON build kieu nay.
all: server client bench

server: server.c proto.c proto.h
	$(CC) $(WARN) $(OPT) -o $@ server.c proto.c -lcrypt

client: client.c proto.c proto.h
	$(CC) $(WARN) $(OPT) -o $@ client.c proto.c

bench: bench.c
	$(CC) $(WARN) $(OPT) -o $@ $<

# Ban debug cho server/client (ASan + UBSan) de phat hien loi bo nho
debug:
	$(CC) $(WARN) -g $(SAN) -o server server.c proto.c -lcrypt
	$(CC) $(WARN) -g $(SAN) -o client client.c proto.c

bench-run: bench
	./bench > results.csv

plot: results.csv
	python3 plot.py results.csv

clean:
	rm -f server client bench

clean-bench:
	rm -f results.csv latency.png throughput.png

distclean: clean clean-bench
