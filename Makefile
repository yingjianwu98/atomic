CC=gcc
CFLAGS=-fPIC -O3 -Wall -Wextra -Iinclude -Werror
LDFLAGS=-libverbs -shared
LIB=libatomic.so
PREFIX=/usr/local

SRC=$(wildcard src/*.c)
OBJ=$(SRC:.c=.o)
BENCH=$(patsubst %.c, %, $(wildcard bench/*.c))
TESTS=$(patsubst %.c, %, $(wildcard tests/*.c))
TEST_CFLAGS=${CFLAGS} #-g -fno-omit-frame-pointer 
TEST_LDFLAGS=-Wl,-rpath,$(shell pwd) $(LIB)

all: build $(TESTS) $(BENCH)

bench: build $(BENCH)

tests: build $(TESTS)

build: $(OBJ)
	$(CC) -o $(LIB) $(OBJ) $(LDFLAGS)

$(TESTS): %: %.c
	$(CC) $< $(TEST_CFLAGS) -o $@ $(TEST_LDFLAGS)

$(BENCH): %: %.c
	$(CC) $< $(CFLAGS) -o $@ $(TEST_LDFLAGS)

src/%.o: src/%.c
	$(CC) -c $< -o $@ $(CFLAGS)

install: all
	cp $(LIB) $(PREFIX)/lib
	cp -r include $(PREFIX)/include/atomic

uninstall:
	$(RM) -f $(PREFIX)/lib/$(LIB)
	$(RM) -rf $(PREFIX)/include/atomic

clean:
	$(RM) -rf $(OBJ) $(TESTS) $(BENCH) $(LIB)

format:
	find . -type f -name "*.c" -o -name "*.h" -exec clang-format -style=llvm -i {} \;

roce:
	modprobe rdma_rxe
	ip link add veth0 type veth peer name veth1
	ip addr add 192.168.2.2/24 dev veth0
	ip addr add 192.168.2.3/24 dev veth1
	ip link set veth0 up
	ip link set veth1 up
	rdma link add rxe0 type rxe netdev veth0
	rdma link add rxe1 type rxe netdev veth1

roce-clean:
	ip link delete veth0
	ip link delete veth2
	ip link delete veth0
	ip link delete veth2
	rdma link delete rxe0
	rdma link delete rxe1
	modprobe -r rdma_rxe
