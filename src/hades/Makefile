CPPFLAGS  := -O3 -I ../libhrd/ -I ../hermes/ #-Wno-unused-result -Wall -Werror
LD      := gcc -O3 -flto
LDFLAGS := ${LDFLAGS} -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CFLAGS   =  -I../../include/libhrd -I../../include/mica -I../../include/hermes
APPS    := hades
PROF    := -g -fno-omit-frame-pointer

all: ${APPS} clean-o

 
hades: hades.o ../aether/aether.o ../libhrd/hrd_conn.o ../libhrd/hrd_util.o
	${LD} -o $@ $^ ${LDFLAGS}

PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ../aether/*.o ${APPS}
clean-o:
	rm -f *.o ../libhrd/*.o ../aether/*.o
