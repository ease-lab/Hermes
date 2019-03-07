LD      := gcc -O3 -flto
LDFLAGS := ${LDFLAGS} -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CFLAGS   =  -I../../include/libhrd -I../../include/mica -I../../include/hermes
APPS    := cr
PROF    := -g -fno-omit-frame-pointer

all: ${APPS} clean-o

 
cr: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o ../mica/mica.o ../mica/city.o ../hermes/main.o cr_wings_worker.o ../hermes/util.o ../hermes/stats.o ../hermes/spacetime.o ../wings/wings.o
	${LD} -o $@ $^ ${LDFLAGS}


PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ../wings/*.o ../hermes/*.o ${APPS}
clean-o:
	rm -f *.o ../libhrd/*.o ../wings/*.o ../hermes/*.o
