#!/usr/bin/env bash
HOSTS=( ##### network  cluster #####
         "houston"
         "sanantonio"
         "austin"
         "indianapolis"
         "atlanta"
         "philly"
         ##### compute cluster #####
#         "chicago"
#         "detroit"
         #"baltimore"
        )

FILES=(
        "hermes"
#        "run-hermes.sh"
      )

LOCAL_HOST=`hostname`
MAKE_FOLDER="/home/user/hermes/src"
HOME_FOLDER="/home/user/hermes/src/hermes"
DEST_FOLDER="/home/user/hermes-exec/src/hermes"

cd $MAKE_FOLDER
make clean
make
cd -

for FILE in "${FILES[@]}"
do
	parallel scp ${HOME_FOLDER}/${FILE} {}:${DEST_FOLDER}/${FILE} ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
	echo "${FILE} copied to {${HOSTS[@]/$LOCAL_HOST}}"
done
