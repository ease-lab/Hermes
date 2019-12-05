#!/usr/bin/env bash

source run.sh

#### Get CLI arguments
# Use -1 for the default (#define in config.h) values if not argument is passed
CREDITS="-1"
NUM_WORKERS="-1"
WRITE_RATIO="-1"
MAX_COALESCE="-1"
MAX_BATCH_SIZE="-1"
RMW_RATIO="-1"
NUM_MACHINES="-1"
LAT_WORKER="-1"

# Each letter is an option argument, if it's followed by a collum
# it requires an argument. The first colum indicates the '\?'
# help/error command when no arguments are given
while getopts ":W:w:l:R:C:c:b:M:h" opt; do
  case $opt in
     W)
       NUM_WORKERS=$OPTARG # Number of threads: this must be smaller than MAX_WORKERS_PER_MACHINE of config.h
       ;;
     w)
       WRITE_RATIO=$OPTARG # given number is divided by 10 to give write rate % (i.e., 55 means 5.5 % writes)
       ;;
     R)
       RMW_RATIO=$OPTARG # percentage of writes to be rmws (i.e., -w 500 -R 500 means 25 % of RMWs and 25% of writes)
                         # RMW is disabled by default (no usage through the artifact) can be enabled through config.h)
       ;;
     C)
       MAX_COALESCE=$OPTARG # maximum number of readily-available messages to be "batched" in a network packet
                            # must be smaller than MTU and it is capped by MAX_REQ_COALESCE in config.h
       ;;
     c)
       CREDITS=$OPTARG      # maximum number of credits per node per thread; credits correspond to messages and not packets
                            # it is capped by MAX_CREDITS_PER_REMOTE_WORKER in config.h
       ;;
     b)
       MAX_BATCH_SIZE=$OPTARG   # amount of requests and protocol messages that can be batched to the KVS
                                # it is capped by MAX_BATCH_KVS_OPS_SIZE in config.h
       ;;
     M)
       NUM_MACHINES=$OPTARG # it is capped by MAX_MACHINE_NUM in config.h and the number of IPS as indicated in hosts.sh
       ;;
     l)
       LAT_WORKER=$OPTARG # An id of the worker who is measuring the latency
                          # if -1 Latency is disabled
                          # otherwise it is capped by running worker threads (NUM_WORKERS-1)
       ;;
     h)
      echo "Usage: -W <# workers> -w <write ratio>  (x1000 --> 10 for 1%)"
      echo "       -c <# credits> -b <max batch size> -C <max coalescing>"
      echo "       -M <# nodes>   -l <latency worker> -R <rmw ratio>"
      exit 1
      ;;
    \?)
      echo "Invalid option: -$OPTARG use -h to get info for arguments" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done


blue "Running hermes threads"
sudo LD_LIBRARY_PATH=/usr/local/lib/ -E \
    ./hermesKV                          \
	--machine-id ${NODE_ID}             \
	--is-roce 0                         \
	--dev-name ${NET_DEVICE_NAME}       \
	--num-machines ${NUM_MACHINES}      \
	--num-workers  ${NUM_WORKERS}       \
	--lat-worker   ${LAT_WORKER}        \
	--rmw-ratio    ${RMW_RATIO}         \
	--write-ratio  ${WRITE_RATIO}       \
	--credits      ${CREDITS}           \
	--max-coalesce ${MAX_COALESCE}      \
	--max-batch-size ${MAX_BATCH_SIZE}  \
	--hermes                            \
	2>&1
