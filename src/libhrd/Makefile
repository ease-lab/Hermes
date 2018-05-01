CPPFLAGS  := -O3   #-Wno-unused-result -Wall -Werror
LD      := gcc
LDFLAGS := ${LDFLAGS} -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CFLAGS=-I../../include/libhrd

APPS    := main

all: ${APPS}

main: hrd_conn.o hrd_util.o main.o
	${LD} -o $@ $^ ${LDFLAGS}
	rm -rf main main.o

PHONY: clean
clean:
	rm -f *.o ${APPS}
