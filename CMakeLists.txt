cmake_minimum_required(VERSION 2.8.12)
project(hermes)

set(Hermes_VERSION_MAJOR 1)
set(Hermes_VERSION_MINOR 0)

include_directories(include/hermes
                    include/libhrd
                    /usr/include/
                    include/optik
        include/mica-herd)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall")

set(SOURCE_FILES_cr
        #Source files
        src/CR/cr_wings_worker.c

        src/wings/wings.c

        src/hermes/main.c
        src/hermes/stats.c
        src/hermes/spacetime.c

        src/mica-herd/mica.c
        src/mica-herd/city.c
        src/mica-herd/hrd_conn.c
        src/mica-herd/hrd_util.c


        ##### header files ####
        include/wings/wings.h
        include/wings/wings_api.h

        include/mica-herd/city.h
        include/mica-herd/hrd.h
        include/mica-herd/sizes.h

        include/hermes/util.h
        include/hermes/config.h
        include/utils/bit_vector.h
        include/utils/concur_ctrl.h
        src/CR/crKV.c)


set(SOURCE_FILES_hades
        #Source files
        src/wings/wings.c
        src/hades/hades.c

        ##### header files ####
        include/wings/wings_api.h
        include/wings/wings.h
        include/hades/hades.h
        src/hades/hades_main.c)


set(SOURCE_FILES_hermes
        #Source files
        src/hermes/main.c
        src/hermes/util.c
#        src/hermes/worker.c
        src/hermes/hermes_worker.c
        src/hermes/stats.c
        src/hermes/spacetime.c
        src/mica-herd/hrd_conn.c
        src/mica-herd/hrd_util.c
        src/mica-herd/mica.c
        src/mica-herd/city.c

        src/wings/wings.c

        ##### header files ####
        include/mica-herd/hrd.h
        include/mica-herd/city.h
        include/mica-herd/sizes.h

        include/hermes/util.h
        include/hermes/config.h
        include/utils/concur_ctrl.h
        include/utils/bit_vector.h
        include/hades/hades.h
        include/wings/wings.h
        include/wings/wings_api.h src/hermes/hermesKV.c)


add_executable(cr ${SOURCE_FILES_cr})
add_executable(hades ${SOURCE_FILES_hades})
add_executable(hermes ${SOURCE_FILES_hermes})
target_link_libraries(cr pthread ibverbs rt memcached numa rdmacm)
target_link_libraries(hades pthread ibverbs rt memcached numa rdmacm)
target_link_libraries(hermes pthread ibverbs rt memcached numa rdmacm)

