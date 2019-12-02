#!/usr/bin/env bash

allIPs=(
### TO BE FILLED: Please provide all cluster IPs
    # Node w/ first IP (i.e., "manager") must run script before the rest of the nodes
    # (instantiates a memcached to setup RDMA connections)
    #
####    network cluster    ####
#          houston
        129.215.165.8
#         sanantonio
        129.215.165.7
#           austin
        129.215.165.9
#        indianapolis
        129.215.165.6
#          philly
        129.215.165.5
#          atlanata
        129.215.165.1
####    compute cluster    ####
#          chicago
        129.215.165.3
#          detroit
        129.215.165.4
#         baltimore
        129.215.165.2
        )

### TO BE FILLED: Modify to get the local IP of the node running the script (must be one of the cluster nodes)
localIP=$(ip addr | grep 'state UP' -A2 | grep 'inet 129.'| awk '{print $2}' | cut -f1  -d'/')

##########################################
### NO NEED TO CHANGE BELOW THIS POINT ###
##########################################

machine_id=-1
echo LOCAL_IP : "$localIP"

for i in "${!allIPs[@]}"; do
	if [  "${allIPs[i]}" ==  "$localIP" ]; then
		machine_id=$i
	else
        remoteIPs+=( "${allIPs[i]}" )
	fi
done

echo Machine-Id "$machine_id"

export HRD_REGISTRY_IP="${allIPs[0]}" # I.E. first IP node (HOUSTON) has a memcached server (used to initialize RDMA QPs)
export MLX5_SINGLE_THREADED=1
export MLX5_SCATTER_TO_CQE=1

sudo killall memcached
sudo killall cr
sudo killall hades
sudo killall hermes

# A function to echo in blue color
function blue() {
	es=`tput setaf 4`
	ee=`tput sgr0`
	echo "${es}$1${ee}"
}


#### free the pages workers use
blue "Removing SHM keys used by HermesKV"
for i in `seq 0 28`; do
	key=`expr 3185 + $i`
	sudo ipcrm -M $key 2>/dev/null
	key=`expr 4185 + $i`
	sudo ipcrm -M $key 2>/dev/null
done
: ${HRD_REGISTRY_IP:?"Need to set HRD_REGISTRY_IP non-empty"}


blue "Reset server QP registry"
memcached -l ${HRD_REGISTRY_IP} 1>/dev/null 2>/dev/null &
sleep 1


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
       NUM_WORKERS=$OPTARG
       ;;
     w)
       WRITE_RATIO=$OPTARG
       ;;
     R)
       RMW_RATIO=$OPTARG
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
    ./hermes                            \
	--machine-id $machine_id            \
	--is-roce 0                         \
	--dev-name "mlx5_0"                 \
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
