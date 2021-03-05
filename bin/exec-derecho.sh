#!/usr/bin/env bash
HOSTS=( ##### network  cluster #####
         "houston"
         "sanantonio"
         "austin"
         "indianapolis"
         "philly"
#         "atlanta"
         ##### compute cluster #####
#         "baltimore"
#         "chicago"
#         "detroit"
        )

NUM_NODES=5
NUM_SENDERS=0 #0 - all senders, 1 - half senders, 2 - one sender
REQS_PER_SENDER=10000000

### Runs to make
#declare -a delivery_mode=(0 1) #0 - ordered mode, 1 - unordered mode
#declare -a object_size=(40 1024)
#declare -a window_size=(128 256)
declare -a delivery_mode=(0) #0 - ordered mode, 1 - unordered mode
declare -a object_size=(256 1024)
declare -a window_size=(128 256)
declare -a iterations=(1 2 3 4) #(1 2 3) for 3 iterations

if [[ $NUM_NODES -ne ${#HOSTS[@]} ]] ; then
    echo "Num_nodes($NUM_NODES) !=  #Hosts(${#HOSTS[@]})"
    exit 1
fi

LOCAL_HOST=`hostname`
HOME_FOLDER="${HOME}/derecho-unified/Release/applications/tests/performance_tests/"
#pin derecho threads to cores (w/o using hyperthreads) of numa node 0
COMMAND_NO_ARGS="taskset -c 0,2,4,6,8,10,12,14,16,18 ./bandwidth_test "

total_iters=0
cd ${HOME_FOLDER} >/dev/null
# Execute locally and remotely
for del_mode in "${delivery_mode[@]}"; do
  for obj_size in "${object_size[@]}"; do
    for win_size in "${window_size[@]}"; do
        for iter in "${iterations[@]}"; do
	        total_iters=$((total_iters + 1))

            args="--DERECHO/max_payload_size=${obj_size} --DERECHO/window_size=${win_size} -- ${NUM_NODES} ${NUM_SENDERS} ${REQS_PER_SENDER} ${del_mode}"
            COMMAND=" ${COMMAND_NO_ARGS} ${args}"

            echo "Running Derecho with: delivery_mode:${del_mode} obj size: $obj_size, window_size: $win_size nodes: $NUM_NODES "
            ${COMMAND} >/dev/null &
            sleep 1

	        parallel "ssh -tt {} $'cd ${HOME_FOLDER}; ${COMMAND}'" ::: $(echo ${HOSTS[@]/$LOCAL_HOST}) >/dev/null
	        sleep 9 # give local node some leeway to log the results into a file
        done
    done
  done
done
tail -${total_iters} data_derecho_bw
cd - >/dev/null
