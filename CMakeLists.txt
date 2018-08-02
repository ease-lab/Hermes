cmake_minimum_required(VERSION 2.8.12)
project(hermes)

set(Hermes_VERSION_MAJOR 1)
set(Hermes_VERSION_MINOR 0)

#include_directories( include/libhrd include/Armonia include/optik include/mica include/herd-hybrid /usr/include/)
#include_directories( include/ccKVS include/rdma_writes include/hermes include/libhrd /usr/include/ include/optik include/mica )
include_directories(include/hermes include/libhrd /usr/include/ include/optik include/mica )

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall ")

set(SOURCE_FILES_spacetime
        #Source files
        src/hermes/main.c
        src/hermes/util.c
        src/hermes/worker.c
        src/hermes/stats.c
        src/libhrd/hrd_conn.c
        src/libhrd/hrd_util.c

        ##### header files ####
        include/libhrd/hrd_sizes.h
        include/libhrd/hrd.h
        include/mica/city.h

        include/hermes/util.h
        include/hermes/config.h src/hermes/spacetime.c)

set(SOURCE_FILES_hermes
        #Source files
        src/rdma_writes/main.c
        src/rdma_writes/util.c
        src/rdma_writes/worker.c
        src/rdma_writes/stats.c
        src/libhrd/hrd_conn.c
        src/libhrd/hrd_util.c

        ##### header files ####
        include/libhrd/hrd_sizes.h
        include/libhrd/hrd.h
        include/mica/city.h

        include/rdma_writes/util.h
        include/rdma_writes/config.h)

set(SOURCE_FILES_ccKVS
        #Source files
        src/ccKVS/main.c
        src/ccKVS/util.c
        #src/ccKVS/mckey.c
        src/ccKVS/cache.c
        src/ccKVS/stats.c
        src/ccKVS/client-sc.c

        src/libhrd/hrd_conn.c
        src/libhrd/hrd_util.c

        ##### header files ####
        include/libhrd/hrd_sizes.h
        include/libhrd/hrd.h
        include/mica/mica.h
        include/mica/city.h
        include/optik/optik_mod.h

        include/ccKVS/cache.h
        include/ccKVS/main.h
        include/ccKVS/inline_util.h
        include/ccKVS/util.h include/hermes/spacetime.h include/hermes/inline-util.h include/hermes/time_rdtsc.h)

add_executable(hermes ${SOURCE_FILES_hermes})
add_executable(ccKVS ${SOURCE_FILES_ccKVS})
add_executable(spacetime ${SOURCE_FILES_spacetime})
target_link_libraries(spacetime pthread ibverbs rt memcached numa rdmacm)

