LD      := gcc -O3 -flto
LDFLAGS := ${LDFLAGS} -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CFLAGS   =  -I../../include/mica-herd -I../../include/hermes
APPS    := cr
PROF    := -g -fno-omit-frame-pointer

all: ${APPS} clean-o

 
cr: ../mica-herd/hrd_conn.o ../mica-herd/hrd_util.o ../mica-herd/mica.o ../mica-herd/city.o ../hermes/main.o cr_wings_worker.o crKV.o ../hermes/spacetime.o ../hermes/util.o ../hermes/stats.o  ../wings/wings.o
	${LD} -o $@ $^ ${LDFLAGS}


PHONY: clean
clean:
	rm -f *.o ../mica-herd/*.o ../wings/*.o ../hermes/*.o ${APPS}
clean-o:
	rm -f *.o ../mica-herd/*.o ../mica-herd/*.o ../wings/*.o ../hermes/*.o