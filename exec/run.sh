#!/usr/bin/env bash

source ./hosts.sh

export HRD_REGISTRY_IP="${ALL_IPS[0]}" # I.E. first IP node (HOUSTON) has a memcached server (used to initialize RDMA QPs)
export MLX5_SINGLE_THREADED=1
export MLX5_SCATTER_TO_CQE=1

sudo killall memcached
sudo killall hades
sudo killall rCRAQ
sudo killall hermesKV

# A function to echo in blue color
function blue() {
	es=`tput setaf 4`
	ee=`tput sgr0`
	echo "${es}$1${ee}"
}


#### free the pages workers use
blue "Removing SHM keys used by HermesKV/rCRAQ"
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
