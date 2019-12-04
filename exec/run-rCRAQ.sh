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
while getopts ":W:w:C:c:b:M:l:h" opt; do
  case $opt in
     W)
       NUM_WORKERS=$OPTARG
       ;;
     w)
       WRITE_RATIO=$OPTARG
       ;;
     C)
       MAX_COALESCE=$OPTARG
       ;;
     c)
       CREDITS=$OPTARG
       ;;
     b)
       MAX_BATCH_SIZE=$OPTARG
       ;;
     M)
       NUM_MACHINES=$OPTARG
       ;;
     l)
       LAT_WORKER=$OPTARG
       ;;
     h)
      echo "Usage: -W <# workers> -w <write ratio>  (x1000 --> 10 for 1%)"
      echo "       -c <# credits> -b <max batch size> -C <max coalescing>"
      echo "       -M <# nodes>   -l <latency worker> "
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
	./rCRAQ                             \
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
	2>&1
