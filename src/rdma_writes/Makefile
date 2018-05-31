CPPFLAGS  := -O3 -I ../libhrd/ -I ../hermes/ #-Wno-unused-result -Wall -Werror
LD      := gcc -O3 -flto
LDFLAGS := ${LDFLAGS} -lgsl -lgslcblas -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CFLAGS   = -I../../include/libhrd -I../../include/rdma_writes
APPS    := rdma_writes
PROF    := -g -fno-omit-frame-pointer

all: ${APPS}

rdma_writes: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o main.o worker.o util.o stats.o
	${LD} -o $@ $^ ${LDFLAGS}


PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ${APPS}
clean-o:
	rm -f *.o ../libhrd/*.o 
