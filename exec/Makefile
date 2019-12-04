CPPFLAGS  := -O3 #-Wno-unused-result -Wall -Werror
LD      := gcc -O3 -flto
LDFLAGS := ${LDFLAGS} -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CFLAGS   =  -I../include/mica-herd -I../include/hermes -I../include/wings -I../include/hades
APPS    := hermesKV rCRAQ
PROF    := -g -fno-omit-frame-pointer

all: ${APPS} clean-o

hermesKV: ../src/wings/wings.o ../src/hades/hades.o \
          ../src/mica-herd/herd.o ../src/mica-herd/mica.o ../src/mica-herd/city.o \
          ../src/hermes/main.o ../src/hermes/hermes_worker.o ../src/hermes/util.o \
          ../src/hermes/stats.o ../src/hermes/spacetime.o ../src/hermes/hermesKV.o
	${LD} -o $@ $^ ${LDFLAGS}


rCRAQ: ../src/mica-herd/herd.o ../src/mica-herd/mica.o \
       ../src/mica-herd/city.o ../src/hermes/main.o ../src/CR/cr_worker.o ../src/CR/crKV.o \
       ../src/hermes/spacetime.o ../src/hermes/util.o ../src/hermes/stats.o  ../src/wings/wings.o
	${LD} -o $@ $^ ${LDFLAGS}


hades-exec: ../src/hades/hades.o ../src/hades/test.o ../src/wings/wings.o ../src/mica-herd/herd.o
	${LD} -o hades $^ ${LDFLAGS}

hades: hades-exec clean-o

PHONY: clean
clean:
	@rm -f ../src/hermes/*.o ../src/mica-herd/*.o ../src/wings/*.o \
	      ../src/CR/*.o ../src/hades/*.o ${APPS} hades

clean-o:
	@rm -f ../src/hermes/*.o ../src/mica-herd/*.o ../src/wings/*.o \
	      ../src/CR/*.o ../src/hades/*.o