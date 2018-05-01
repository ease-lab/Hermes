CPPFLAGS  := -O3 -I ../libhrd/ -I ../mica/ #-Wno-unused-result -Wall -Werror
LD      := gcc -O3 -flto
LDFLAGS := ${LDFLAGS} -lgsl -lgslcblas -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CFLAGS   = -I../../include/mica -I../../include/libhrd -I../../include/ccKVS -I../../include/optik
APPS    := ccKVS-sc clean-o #ccKVS-ec
PROF    := -g -fno-omit-frame-pointer

all: ${APPS}

ccKVS-sc: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o ../mica/mica.o ../mica/city.o main.o client-sc.o worker.o cache.o util.o stats.o 
	${LD} -o $@ $^ ${LDFLAGS}

#ccKVS-ec: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o ../mica/mica.o ../mica/city.o main.o client-ec.o worker.o cache.o util.o stats.o 
	#${LD}  -o $@ $^ ${LDFLAGS}


PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ../mica/*.o ${APPS}
clean-o:
	rm -f *.o ../libhrd/*.o ../mica/*.o
