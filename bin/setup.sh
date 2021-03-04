#!/usr/bin/env bash
# Exec this script in every cluster node after you have
# installed the (Infiniband) Verbs drivers through Mellanox OFED:
# 1. Download the MLNX_OFED (tested on --> MLNX_OFED_LINUX-4.4-2.0.7.0-ubuntu18.04-x86_64)
#    https://www.mellanox.com/page/products_dyn?product_family=26
# 2. tar -xvf the tar file
# 3. install through --> sudo ./mlnxofedinstall

if ! [ -x "$(command -v ofed_info)" ]; then
    echo "Error: mellanox ofed is not installed." >&2
    echo " Please install the (Infiniband) Verbs drivers through Mellanox OFED by:"
    echo "  1. Download the MLNX_OFED (tested on --> MLNX_OFED_LINUX-4.4-2.0.7.0-ubuntu18.04-x86_64)"
    echo "     https://www.mellanox.com/page/products_dyn?product_family=26"
    echo "  2. tar -xvf the tar file"
    echo "  3. install through --> sudo ./mlnxofedinstall"
    exit 1
else
    MLNX_OFED_VERSION=`ofed_info | head -1`
    echo "Running OFED driver version: ${MLNX_OFED_VERSION}" >&2
fi

# Install required Libraries (memcached is used to setup RDMA connection and numa for mbind)
sudo apt install libmemcached-dev libnuma-dev memcached

# start a subnet manager
sudo /etc/init.d/opensmd start # there must be at least one subnet-manager in an infiniband subnet cluster
# start the driver
sudo /etc/init.d/openibd start

# Configure (2MB) huge-pages for the KVS
# Note that such a huge page allocation is not permanent and must be re-applied after a node reboot.
echo 8192 | sudo tee /sys/devices/system/node/node*/hugepages/hugepages-2048kB/nr_hugepages
echo 10000000001 | sudo tee /proc/sys/kernel/shmmax
echo 10000000001 | sudo tee /proc/sys/kernel/shmall
