#!/usr/bin/env bash

EXEC_FOLDER="/home/${USER}/hermes/exec"
RESULTS_FOLDER="/home/${USER}/hermes/exec/results"

RESULT_FOLDER="${RESULTS_FOLDER}/xput/per-node/"
RESULT_OUT_FOLDER="${RESULTS_FOLDER}/xput/per-node/"
RESULT_OUT_FOLDER_MERGE="${RESULTS_FOLDER}/xput/all-nodes/"

cd ${EXEC_FOLDER} >/dev/null
# get Hosts
source ./hosts.sh
cd - >/dev/null

# Gather remote files
parallel "scp {}:${RESULT_FOLDER}* ${RESULT_OUT_FOLDER} " ::: $(echo ${REMOTE_HOSTS[@]})
echo "xPut result files copied from: {${REMOTE_HOSTS}}"

# group all files
ls ${RESULT_OUT_FOLDER} | awk -F '-' '!x[$2]++{print $1}' | while read -r line; do
    # Create an intermediate file print the 3rd line for all files with the same prefix to the same file
    awk 'FNR==3 {print $0}' ${RESULT_OUT_FOLDER}/$line* > ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt
          #   Sum up the xPut of the (3rd iteration) from every node to create the final file
    awk -F ':' '{sum += $2} END {print sum}' ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt > ${RESULT_OUT_FOLDER_MERGE}/$line.txt
    rm -rf  ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt
done

echo "System-wide xPut results produced in ${RESULT_OUT_FOLDER_MERGE} directory!"
