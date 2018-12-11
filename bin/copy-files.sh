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
        "hermes"
#        "run-hermes.sh"
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


## TODO this is only for indianapolis
#DEST_FOLDER_2="/home/user/hermes-exec/src/hermes"
#for FILE in "${FILES[@]}"
#do
#	scp ${HOME_FOLDER}/${FILE} user@indianapolis:${DEST_FOLDER_2}/${FILE}
#	echo "${FILE} copied to {indianapolis}}"
#done
