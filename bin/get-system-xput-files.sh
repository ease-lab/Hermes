#!/usr/bin/env bash
HOSTS=( ##### network  cluster #####
         "houston"
         "sanantonio"
         "austin"
         "indianapolis"
         "philly"
         "atlanta"
         ##### compute cluster #####
#         "baltimore"
#         "chicago"
#         "detroit"
        )

USERNAME="s1671850" # "user"
LOCAL_HOST=`hostname`
EXEC_FOLDER="/home/${USERNAME}/hermes/exec"
RESULTS_FOLDER="/home/${USERNAME}/hermes/exec/results"

RESULT_FOLDER="${RESULTS_FOLDER}/xput/per-node/"
RESULT_OUT_FOLDER="${RESULTS_FOLDER}/xput/per-node/"
RESULT_OUT_FOLDER_MERGE="${RESULTS_FOLDER}/xput/all-nodes/"

# Gather remote files
parallel "scp {}:${RESULT_FOLDER}* ${RESULT_OUT_FOLDER} " ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
echo "xPut result files copied from: {${HOSTS[@]/$LOCAL_HOST}}"

# group all files
ls ${RESULT_OUT_FOLDER} | awk -F '-' '!x[$2]++{print $1}' | while read -r line; do
    # Create an intermediate file print the 3rd line for all files with the same prefix to the same file
    awk 'FNR==3 {print $0}' ${RESULT_OUT_FOLDER}/$line* > ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt
          #   Sum up the xPut of the (3rd iteration) from every node to create the final file
    awk -F ':' '{sum += $2} END {print sum}' ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt > ${RESULT_OUT_FOLDER_MERGE}/$line.txt
    rm -rf  ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt
done

echo "System-wide xPut results produced in ${RESULT_OUT_FOLDER_MERGE} directory!"
