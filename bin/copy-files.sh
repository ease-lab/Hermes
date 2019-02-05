#!/usr/bin/env bash
HOSTS=( ##### network  cluster #####
         "houston"
         "sanantonio"
         "austin"
#         "indianapolis"
#         "atlanta"
#         "philly"
         ##### compute cluster #####
#         "chicago"
#         "detroit"
         #"baltimore"
        )

FILES=(
#        "hermes"
        "hermes-aether"
        "run-hermes.sh"
      )

USERNAME="s1671850" # "user"
LOCAL_HOST=`hostname`
MAKE_FOLDER="/home/${USERNAME}/hermes/src"
HOME_FOLDER="/home/${USERNAME}/hermes/src/hermes"
DEST_FOLDER="/home/${USERNAME}/hermes-exec/src/hermes"

cd $MAKE_FOLDER
make clean
make
cd -

for FILE in "${FILES[@]}"
do
	parallel scp ${HOME_FOLDER}/${FILE} {}:${DEST_FOLDER}/${FILE} ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
	echo "${FILE} copied to {${HOSTS[@]/$LOCAL_HOST}}"
done

