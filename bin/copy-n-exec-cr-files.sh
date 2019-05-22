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

FILES=(
        "run-cr.sh"
        "cr"
      )

USE_SAME_BATCH_N_CREDITS=1

### Runs to make
#declare -a write_ratios=(0 10 50 200 500 1000)
#declare -a write_ratios=(500 750 1000)
#declare -a write_ratios=(10 20 50)
declare -a write_ratios=(200 500 750 1000)
#declare -a write_ratios=(500)
#declare -a num_workers=(5 10 15 20 25 30 36)
declare -a num_workers=(39)
#declare -a batch_sizes=(25 50 75 100 125 150 200 250)
declare -a batch_sizes=(50 125 250)
#declare -a credits=(250) # make sure credits % NUM_NODES == 0
declare -a credits=(125) # make sure credits % NUM_NODES == 0
#declare -a credits=(30)
#declare -a coalesce=(1 5 10 15)
declare -a coalesce=(10)

USERNAME="s1671850" # "user"
LOCAL_HOST=`hostname`
MAKE_FOLDER="/home/${USERNAME}/hermes/src"
HOME_FOLDER="/home/${USERNAME}/hermes/src/CR"
DEST_FOLDER="/home/${USERNAME}/hermes-exec/src/CR"
RESULT_FOLDER="/home/${USERNAME}/hermes-exec/results/xput/per-node/"
RESULT_OUT_FOLDER="/home/${USERNAME}/hermes/results/xput/per-node/"
RESULT_OUT_FOLDER_MERGE="/home/${USERNAME}/hermes/results/xput/all-nodes/"

cd $MAKE_FOLDER
make clean
make
cd -

for FILE in "${FILES[@]}"
do
	parallel scp ${HOME_FOLDER}/${FILE} {}:${DEST_FOLDER}/${FILE} ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
	echo "${FILE} copied to {${HOSTS[@]/$LOCAL_HOST}}"
done

REMOTE_COMMAND="cd ${DEST_FOLDER}; bash run-cr.sh"
OUTP_FOLDER="/home/${USERNAME}/hermes/results/xput/per-node/"

PASS="${1}"
if [ -z "$PASS" ]
then
      echo "\$PASS is empty! --> sudo pass for remotes is expected to be the first arg"
      exit;
fi
#else
      echo "\$PASS is OK!"
#	  parallel "echo ${PASS} | ssh -tt {} $'${REMOTE_COMMAND}' | cat" ::: sanantonio
#	  parallel "echo ${PASS} | ssh -tt {} $'${REMOTE_COMMAND}' > ${OUTP_FOLDER}/{}.out" ::: sanantonio
cd ${HOME_FOLDER}
#      echo ${PASS} | sudo -S bash ./run-hermes.sh > ${OUTP_FOLDER}/${LOCAL_HOST}.out &

#	  parallel "echo ${PASS} | ssh -tt {} $'${REMOTE_COMMAND}' > ${OUTP_FOLDER}/{}.out" ::: $(echo ${HOSTS[@]/$LOCAL_HOST}) &
#      echo ${PASS} | ./run-hermes.sh > ${OUTP_FOLDER}/${LOCAL_HOST}.out

if [ ${USE_SAME_BATCH_N_CREDITS} -eq 0 ]
then
       # Execute locally and remotely
       for WR in "${write_ratios[@]}"; do
        for W in "${num_workers[@]}"; do
          for BA in "${batch_sizes[@]}"; do
            for CRD in "${credits[@]}"; do
              for COAL in "${coalesce[@]}"; do
                 args=" -w ${WR} -W ${W} -b ${BA} -c ${CRD} -C ${COAL}"
                 echo ${PASS} | ./run-cr.sh ${args} &
                 sleep 2
	             parallel "echo ${PASS} | ssh -tt {} $'${REMOTE_COMMAND} ${args}'" ::: $(echo ${HOSTS[@]/$LOCAL_HOST}) >/dev/null
	          done
	        done
	      done
	    done
	   done

else
       # Execute locally and remotely
       for WR in "${write_ratios[@]}"; do
        for W in "${num_workers[@]}"; do
          for BA in "${batch_sizes[@]}"; do
              for COAL in "${coalesce[@]}"; do
                 args=" -w ${WR} -W ${W} -b ${BA} -c ${BA} -C ${COAL}"
                 echo ${PASS} | ./run-cr.sh ${args} &
                 sleep 2
	             parallel "echo ${PASS} | ssh -tt {} $'${REMOTE_COMMAND} ${args}'" ::: $(echo ${HOSTS[@]/$LOCAL_HOST}) >/dev/null
	          done
	        done
	      done
	    done
fi

# Gather remote files
parallel "scp {}:${RESULT_FOLDER}* ${RESULT_OUT_FOLDER} " ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
echo "xPut result files copied from: {${HOSTS[@]/$LOCAL_HOST}}"

	  # group all files
ls ${RESULT_OUT_FOLDER} | awk -F '-' '!x[$1]++{print $1}' | while read -r line; do
        #// Create an intermediate file print the 3rd line for all files with the same prefix to the same file
    awk 'FNR==3 {print $0}' ${RESULT_OUT_FOLDER}/$line* > ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt
    #   Sum up the xPut of the (3rd iteration) from every node to create the final file
    awk -F ':' '{sum += $2} END {print sum}' ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt > ${RESULT_OUT_FOLDER_MERGE}/$line.txt
    rm -rf  ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt
done

cd -


