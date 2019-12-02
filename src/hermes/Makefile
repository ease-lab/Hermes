CPPFLAGS  := -O3 -I ../mica-herd/ -I ../hermes/ #-Wno-unused-result -Wall -Werror
LD      := gcc -O3 -flto
LDFLAGS := ${LDFLAGS} -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CFLAGS   =  -I../../include/mica-herd -I../../include/hermes -I../../include/wings
APPS    := hermes
PROF    := -g -fno-omit-frame-pointer

all: ${APPS} clean-o

hermes: ../wings/wings.o ../hades/hades.o \
        ../mica-herd/herd.o ../mica-herd/mica.o ../mica-herd/city.o     \
        main.o hermes_worker.o util.o stats.o spacetime.o hermesKV.o
	${LD} -o $@ $^ ${LDFLAGS}

PHONY: clean
clean:
	rm -f *.o ../mica-herd/*.o ../wings/*.o ${APPS}

clean-o:
	rm -f *.o ../mica-herd/*.o ../wings/*.o
