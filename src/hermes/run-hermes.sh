#!/usr/bin/env bash

allIPs=(
            ####    network cluster    ####
#          houston      sanantonio      austin
        129.215.165.8 129.215.165.7 129.215.165.9
#        indianapolis
        129.215.165.6
#          philly
        129.215.165.5
#          atlanata
        129.215.165.1
            ####    compute cluster    ####
#          chicago       detroit      baltimore
        129.215.165.3 129.215.165.4 129.215.165.2
        )
localIP=$(ip addr | grep 'state UP' -A2 | grep 'inet 129.'| awk '{print $2}' | cut -f1  -d'/')


echo LOCAL_IP : "$localIP"
machine_id=-1

for i in "${!allIPs[@]}"; do
	if [  "${allIPs[i]}" ==  "$localIP" ]; then
		machine_id=$i
	else
        remoteIPs+=( "${allIPs[i]}" )
	fi
done


#echo AllIps: "${allIPs[@]}"
#echo RemoteIPs: "${remoteIPs[@]}"
echo Machine-Id "$machine_id"


export HRD_REGISTRY_IP="129.215.165.8" # I.E. HOUSTON
export MLX5_SINGLE_THREADED=1
export MLX5_SCATTER_TO_CQE=1

sudo killall memcached
sudo killall hermes
# A function to echo in blue color
function blue() {
	es=`tput setaf 4`
	ee=`tput sgr0`
	echo "${es}$1${ee}"
}

# free the pages workers use
blue "Removing SHM keys used by Spacetime / MICA KV"
for i in `seq 0 28`; do
	key=`expr 3185 + $i`
	sudo ipcrm -M $key 2>/dev/null
	key=`expr 4185 + $i`
	sudo ipcrm -M $key 2>/dev/null
done
: ${HRD_REGISTRY_IP:?"Need to set HRD_REGISTRY_IP non-empty"}


blue "Removing hugepages"
shm-rm.sh 1>/dev/null 2>/dev/null


blue "Reset server QP registry"
memcached -l 0.0.0.0 1>/dev/null 2>/dev/null &
sleep 1

blue "Running hermes threads"

sudo LD_LIBRARY_PATH=/usr/local/lib/ -E     \
#	./hermes                            \
        #valgrind --leak-check=yes 	    \
        ./hermes-wings                      \
	--machine-id $machine_id            \
	--is-roce 0                         \
	--dev-name "mlx5_0"                 \
	2>&1
