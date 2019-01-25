CPPFLAGS  := -O3 -I ../libhrd/ -I ../hermes/ #-Wno-unused-result -Wall -Werror
LD      := gcc -O3 -flto
LDFLAGS := ${LDFLAGS} -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CFLAGS   =  -I../../include/libhrd -I../../include/mica -I../../include/hermes
APPS    := hermes
PROF    := -g -fno-omit-frame-pointer

all: ${APPS} clean-o

 
hermes: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o ../mica/mica.o ../mica/city.o main.o worker.o util.o stats.o spacetime.o ../aether/ud-wrapper.o
	${LD} -o $@ $^ ${LDFLAGS}
	
PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ${APPS}
clean-o:
	rm -f *.o ../libhrd/*.o
