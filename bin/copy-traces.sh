#!/usr/bin/env bash
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly" )
HOSTS=( "austin" "houston" "sanantonio")
HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly" "baltimore" "chicago" "atlanta" "detroit")
LOCAL_HOST=`hostname`
EXECUTABLES=( "traces" ) #"run-hermes.sh" )
HOME_FOLDER="/home/user/hermes/"
DEST_FOLDER="/home/user/hermes-exec/"

cd $HOME_FOLDER
make
cd -

for EXEC in "${EXECUTABLES[@]}"
do
	#echo "${EXEC} copied to {${HOSTS[@]/$LOCAL_HOST}}"
	parallel scp -r ${HOME_FOLDER}/${EXEC} {}:${DEST_FOLDER}/${EXEC} ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
	echo "${EXEC} copied to {${HOSTS[@]/$LOCAL_HOST}}"
done
