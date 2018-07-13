#!/usr/bin/env bash

num_machines=2 #3
#allIPs=(129.215.165.8 129.215.165.7 129.215.165.9 129.215.165.6 129.215.165.5) # houston-sanantonio-austin-indianapolis-philly
#houston-sanantonio-austin-indianapolis-philly-chicago-detroit-baltimore-atlanta
allIPs=(129.215.165.8 129.215.165.7 129.215.165.9 129.215.165.6 129.215.165.5  129.215.165.3 129.215.165.4 129.215.165.2 129.215.165.1)
localIP=$(ip addr | grep 'state UP' -A2 | sed -n 3p | awk '{print $2}' | cut -f1  -d'/')
#$(ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1  -d'/')

tmp=$((${#localIP}-1))
machine_id=-1 #$((${localIP:$tmp:1}-1))

# <vasilis>
for i in "${!allIPs[@]}"; do
	if [  "${allIPs[i]}" ==  "$localIP" ]; then
		machine_id=$i
	else
    remoteIPs+=( "${allIPs[i]}" )
	fi
done
# </vasilis>


#echo AllIps: "${allIPs[@]}"
#echo RemoteIPs: "${remoteIPs[@]}"
echo Machine-Id "$machine_id"


#export HRD_REGISTRY_IP="129.215.165.8" # I.E. HOUSTON
export HRD_REGISTRY_IP="129.215.165.7" # I.E. SANANTONIO
export MLX5_SINGLE_THREADED=1
export MLX5_SCATTER_TO_CQE=1

sudo killall memcached
sudo killall spacetime
# A function to echo in blue color
function blue() {
	es=`tput setaf 4`
	ee=`tput sgr0`
	echo "${es}$1${ee}"
}


#blue "Removing SHM keys used by the workers 24 -> 24 + Workers_per_machine (request regions hugepages)"
#for i in `seq 0 32`; do
#	key=`expr 24 + $i`
#	sudo ipcrm -M $key 2>/dev/null
#done

# free the  pages workers use
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
#sudo killall memcached
memcached -l 0.0.0.0 1>/dev/null 2>/dev/null &
sleep 1

blue "Running hermes threads"

sudo LD_LIBRARY_PATH=/usr/local/lib/ -E \
	./hermes \
	--machine-id $machine_id \
	--is-roce 0 \
	2>&1
