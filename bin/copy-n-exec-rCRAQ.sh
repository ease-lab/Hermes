#!/usr/bin/env bash

USE_SAME_BATCH_N_CREDITS=0

### Runs to make
declare -a write_ratios=(1000)
#declare -a num_workers=(5 10 15 20 25 30 36)
declare -a num_workers=(1)
#declare -a batch_sizes=(25 50 75 100 125 150 200 250)
declare -a batch_sizes=(50)
declare -a credits=(15) # WARNING credits for CR must be divided by the num_machines (i.e., credits % num_machines == 0)
#declare -a coalesce=(1 5 10 15)
declare -a coalesce=(10)
#declare -a num_machines=(2 3 5 7)
declare -a num_machines=(3)

# Set LAT_WORKER to -1 to disable latency measurement or to worker id (i.e., from 0 up to [num-worker - 1])
LAT_WORKER="-1"
#LAT_WORKER="0"

#LOCAL_HOST=`hostname`
EXEC_FOLDER="/home/${USER}/hermes/exec"
REMOTE_COMMAND="cd ${EXEC_FOLDER}; bash run-rCRAQ.sh"

PASS="${1}"
if [ -z "$PASS" ]
then
      echo "\$PASS is empty! --> sudo pass for remotes is expected to be the first arg"
      exit;
fi

echo "\$PASS is OK!"
cd ${EXEC_FOLDER}

# get Hosts
source ./hosts.sh

../bin/copy-exec-files.sh

if [ ${USE_SAME_BATCH_N_CREDITS} -eq 0 ]
then
   for M in "${num_machines[@]}"; do
       # Execute locally and remotely
       for WR in "${write_ratios[@]}"; do
        for W in "${num_workers[@]}"; do
          for BA in "${batch_sizes[@]}"; do
            for CRD in "${credits[@]}"; do
              for COAL in "${coalesce[@]}"; do
                 args=" -M ${M} -w ${WR} -W ${W} -b ${BA} -c ${CRD} -C ${COAL} -l ${LAT_WORKER}"
                 echo ${PASS} | ./run-rCRAQ.sh ${args} &
                 sleep 2
	             parallel "echo ${PASS} | ssh -tt {} $'${REMOTE_COMMAND} ${args}'" ::: $(echo ${REMOTE_HOSTS[@]}) >/dev/null
	          done
	        done
	      done
	    done
	   done
   done

else
       # Execute locally and remotely
   for M in "${num_machines[@]}"; do
       for WR in "${write_ratios[@]}"; do
        for W in "${num_workers[@]}"; do
          for BA in "${batch_sizes[@]}"; do
              for COAL in "${coalesce[@]}"; do
                 args=" -M ${M} -w ${WR} -W ${W} -b ${BA} -c ${BA} -C ${COAL} -l ${LAT_WORKER}"
                 echo ${PASS} | ./run-rCRAQ.sh ${args} &
                 sleep 2
	             parallel "echo ${PASS} | ssh -tt {} $'${REMOTE_COMMAND} ${args}'" ::: $(echo ${REMOTE_HOSTS[@]}) >/dev/null
	          done
	        done
	      done
	    done
   done
fi

cd - >/dev/null

../bin/get-system-xput-files.sh
