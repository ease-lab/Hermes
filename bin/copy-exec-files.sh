#!/usr/bin/env bash
HOSTS=( ##### network  cluster #####
         "houston"
         "sanantonio"
         "austin"
         "indianapolis"
         "atlanta"
         "philly"
         ##### compute cluster #####
#         "baltimore"
#         "chicago"
#         "detroit"
        )

FILES=(
        "run-hermesKV.sh"
        "hermesKV"
        "run-rCRAQ.sh"
        "rCRAQ"
      )

USERNAME="s1671850" # "user"
LOCAL_HOST=`hostname`
EXEC_FOLDER="/home/${USERNAME}/hermes/exec"

cd $EXEC_FOLDER
make clean; make
cd -

for FILE in "${FILES[@]}"
do
	parallel scp ${EXEC_FOLDER}/${FILE} {}:${EXEC_FOLDER}/${FILE} ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
	echo "${FILE} copied to {${HOSTS[@]/$LOCAL_HOST}}"
done

