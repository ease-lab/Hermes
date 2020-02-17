# Hermes Reliable Replication Protocol

<img align="left" height="90" src="https://github.com/akatsarakis/Hermes/blob/master/hermes-logo.png">

This is the publicly available artifact repository supporting the ASPLOS'20 paper [_"Hermes: A Fast, Fault-Tolerant and Linearizable Replication Protocol"_](https://arxiv.org/abs/2001.09804 "Hermes Arxiv version"). The repository contains both code to experimentally evaluate Hermes(KV) and complete Hermes TLA+ specifications which can be used to verify Hermes correctness via model-checking.

----
## High Perfomance Features
- _Reads_: i) Local ii) Load-balanced (served by any replica)
- _Updates (Writes and RMWs)_: i) Inter-key concurrent ii) Decentralized iii) Fast (1rtt commit -- any replica)
- _Writes_: iv) Non-conflicting (i.e., never abort)

## Linearizablity and Properties
Linearizable reads, writes and RMWs with the following properties:
1. _Writes_: from a live replica _always commit_ after Invalidating (and getting acknowledgments from) the rest live replicas. 
1. _RMWs_: at most one of possible concurrent RMWs to a key can commit, and this only once all acknowledgments from live replicas are gathered.
1. _Reads_: return the local value if the targeted keys are found in the Valid state and the coordinator was considered live at the time of reading. The former can be ensured locally if the coordinator has a lease for (and is part of) the membership.

## Fault Tolerance
Coupling Invalidations with per-key logical timestamps (i.e., Lamport clocks) and propagating the value to be updated with the invalidation message (_early value propagation_), Hermes allows any replica blocked by an update (write or RMW) to safely replay the update and unblock it self and the rest of followers.

----

## License

This work is freely distributed under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0 "Apache 2.0").  
The authors encourage any capital contributions to be donated to charity instead!


## Hardware dependencies

A homogeneous cluster of x86_64 nodes interconnected via RDMA network cards and switched 
(tested on "Mellanox ConnectX-4" Infiniband infrastructure).


## Software requirements

Linux OS (tested on Ubuntu 18.04 4.15.0-55-generic) with root access.

The software is tested using the following version of Mellanox OFED RDMA drivers
`MLNX_OFED_LINUX-4.4-2.0.7.0`.

Third-party libraries that you will require to run the experiments include:
1. _parallel_ (Cluster management scripts only)
1. _libmemcached-dev_ (used to exchange QP informations for the setup of RDMA connections)
1. _libnuma-dev_	(for mbind)


## Setup

On every node:
1. Install Mellanox OFED ibverbs drivers
1. `./hermes/bin/setup.sh`

On manager (just pick on node in the cluster):
1. Fill variables in `/hermes/exec/hosts.sh`
1. Configure setup and default parameters in `/hermes/include/hermes/config.h`
1. From `/hermes/exec/` compile _hermesKV_ through make
1. scp  _hermesKV_ and the configured hosts.sh in the `/hermes/exec/` directory of all other nodes in the cluster. 


## Compilation

`cd hermes/exec; make`

_Warning_: Do not compile through through cmake; instead use the Makefile in exec/ directory.


## Run

Run first on manager:
`./run-hermesKV.sh <experiment_parameters>`

Then run on all other member nodes 
`./run-hermesKV.sh <experiment_parameters>`

> Note that some members will eagerly terminate if experiment 
  uses smaller number of nodes than specified in hosts.sh
  
An experiment example for three nodes 12 worker threads and 35% write ratio would be as follows:
`./run-hermesKV.sh -W 12 -w 350 -M 3`
Supported command-line arguments for the experiments are detailed in the run-hermesKV.sh script.

---
## Contact
 Antonios Katsarakis: `antoniskatsarakis@yahoo.com`
