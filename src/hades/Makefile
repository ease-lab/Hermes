LD      := gcc -O3 -flto
LDFLAGS := ${LDFLAGS} -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CFLAGS   =  -I../../include/mica-herd -I../../include/hermes -I../../include/wings
APPS    := hades
PROF    := -g -fno-omit-frame-pointer

all: ${APPS} clean-o

 
hades: hades.o test.o ../wings/wings.o ../mica-herd/herd.o
	${LD} -o $@ $^ ${LDFLAGS}

PHONY: clean
clean:
	rm -f *.o ../mica-herd/*.o ../wings/*.o ${APPS}
clean-o:
	rm -f *.o ../mica-herd/*.o ../wings/*.o
