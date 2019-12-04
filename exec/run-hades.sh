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
sudo killall hades
sudo killall rCRAQ
sudo killall hermesKV

# A function to echo in blue color
function blue() {
	es=`tput setaf 4`
	ee=`tput sgr0`
	echo "${es}$1${ee}"
}

# free the  pages workers use
blue "Removing SHM keys used by Spacetime / MICA KV"
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

blue "Running hades"

sudo LD_LIBRARY_PATH=/usr/local/lib/ -E \
	./hades                             \
	--machine-id $machine_id            \
	--dev-name "mlx5_0"                 \
	2>&1
