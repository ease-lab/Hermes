cmake_minimum_required(VERSION 2.8.12)
project(hermes)

set(Hermes_VERSION_MAJOR 1)
set(Hermes_VERSION_MINOR 0)

include_directories(include/hermes include/libhrd /usr/include/ include/optik include/mica )
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall")

set(SOURCE_FILES_hermes
        #Source files
        src/hermes/main.c
        src/hermes/util.c
#        src/hermes/worker.c
        src/hermes/aether_worker.c
        src/hermes/stats.c
        src/hermes/spacetime.c
        src/libhrd/hrd_conn.c
        src/libhrd/hrd_util.c
        src/mica/mica.c
        src/mica/city.c

        ##### header files ####
        include/libhrd/hrd.h
        include/mica/city.h
        include/libhrd/sizes.h

        include/hermes/util.h
        include/hermes/config.h
        include/hermes/concur_ctrl.h
        include/hermes/bit_vector.h
        include/hades/hades.h
        src/aether/ud-wrapper.c
        src/aether/ud-wrapper.h)


add_executable(hermes ${SOURCE_FILES_hermes})
target_link_libraries(hermes pthread ibverbs rt memcached numa rdmacm)

