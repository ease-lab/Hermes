#!/usr/bin/env bash

# Libraries required
sudo apt install libmemcached-dev libnuma-dev

# Exec the following in every node
echo 8192 | sudo tee /sys/devices/system/node/node*/hugepages/hugepages-2048kB/nr_hugepages
echo 10000000001 | sudo tee /proc/sys/kernel/shmmax
echo 10000000001 | sudo tee /proc/sys/kernel/shmall

echo "Verify results:"
#Verify results
cat /sys/devices/system/node/node*/hugepages/hugepages-2048kB/nr_hugepages
cat /proc/sys/kernel/shmmax
cat /proc/sys/kernel/shmall

# Exec the following in every node
sudo /etc/init.d/opensmd start
sudo /etc/init.d/openibd start

#PCIe counter settings
echo 0 > /proc/sys/kernel/nmi_watchdog
modprobe msr
