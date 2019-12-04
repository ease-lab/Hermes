#!/usr/bin/env bash
#HOSTS=( ##### network  cluster #####
#         "houston"
#         "sanantonio"
#         "austin"
#         "indianapolis"
#         "atlanta"
#         "philly"
#         ##### compute cluster #####
##         "baltimore"
##         "chicago"
##         "detroit"
#        )
#
#LOCAL_HOST=`hostname`

ALL_IPS=(
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
LOCAL_IP=$(ip addr | grep 'state UP' -A2 | grep 'inet 129.'| awk '{print $2}' | cut -f1  -d'/')
#LOCAL_IP="129.215.164.2"


##########################################
### NO NEED TO CHANGE BELOW THIS POINT ###
##########################################

REMOTE_IPS=${ALL_IPS[@]/$LOCAL_IP}
REMOTE_HOSTS=${ALL_IPS[@]/$LOCAL_IP}

NODE_ID=-1

for i in "${!ALL_IPS[@]}"; do
	if [  "${ALL_IPS[i]}" ==  "$LOCAL_IP" ]; then
		NODE_ID=$i
	fi
done


if [[ ${NODE_ID} == -1 ]]; then
    echo "Error Local IP: ${LOCAL_IP} n is not in ALL_IPS:"
    echo "    {${ALL_IPS[@]}}"
    exit
fi

echo "Local node id:" ${NODE_ID}
