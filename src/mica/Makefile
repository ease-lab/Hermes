CFLAGS  := -O3  -I ../libhrd/ # -Wno-unused-result  -Wall -Werror
LD      := gcc -O3
LDFLAGS := ${LDFLAGS} -lrt -lpthread -lnuma -libverbs -lmemcached  -lrdmacm#-lpapi
CFLAGS  = -I../../include/mica -I../../include/libhrd
APPS    := test

all: ${APPS}

test: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o mica.o city.o test.o
	${LD} -o $@ $^ ${LDFLAGS}

PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ${APPS}
