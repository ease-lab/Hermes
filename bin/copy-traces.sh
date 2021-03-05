#!/usr/bin/env bash

# Copy (per-thread splitted) trace folder
FOLDERS_TO_CPY=( "traces/current-splitted-traces" )
HOME_FOLDER="${HOME}/hermes"

cd ${HOME_FOLDER} >/dev/null
# get Hosts
source ./exec/hosts.sh
cd - >/dev/null

for FOLDER in "${FOLDERS_TO_CPY[@]}"
do
	parallel scp -r ${HOME_FOLDER}/${FOLDER} {}:${HOME_FOLDER}/${FOLDER} ::: $(echo ${REMOTE_HOSTS[@]})
	echo "${FOLDER} copied to {${REMOTE_HOSTS[@]}}"
done
