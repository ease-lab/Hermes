#!/usr/bin/env bash


ALL_IPS=(
### TO BE FILLED: Please provide all cluster IPs
    # Node w/ first IP (i.e., "manager") must run script before the rest of the nodes
    # (instantiates a memcached to setup RDMA connections)
    #
        10.0.3.1
        10.0.3.2
        10.0.3.3
        10.0.3.4
        10.0.3.5
        )

### TO BE FILLED: Modify to get the local IP of the node running the script (must be one of the cluster nodes)
LOCAL_IP=$(ip addr | grep 'state UP' -A2 | grep 'inet 10.0.3'| awk '{print $2}' | cut -f1  -d'/')
#LOCAL_IP="129.215.164.2"

### Fill the RDMA device name (the "hca_id" of the device when executing ibv_devinfo)
#NET_DEVICE_NAME="mlx5_0"
NET_DEVICE_NAME="mlx4_0"

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
