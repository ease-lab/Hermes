#!/usr/bin/env bash
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly" )
HOSTS=( "austin" "houston" "sanantonio")
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly" "baltimore" "chicago" "atlanta" "detroit")
LOCAL_HOST=`hostname`
EXECUTABLES=( "traces/current-splited-traces" )
HOME_FOLDER="/home/s1671850/hermes/"
DEST_FOLDER="/home/s1671850/hermes-exec/"

for EXEC in "${EXECUTABLES[@]}"
do
	parallel scp -r ${HOME_FOLDER}/${EXEC} {}:${DEST_FOLDER}/${EXEC} ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
	echo "${EXEC} copied to {${HOSTS[@]/$LOCAL_HOST}}"
done
