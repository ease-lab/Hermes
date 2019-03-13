#!/usr/bin/env bash
HOSTS=( ##### network  cluster #####
         "houston"
         "sanantonio"
         "austin"
#         "indianapolis"
#         "philly"
#         "atlanta"
         ##### compute cluster #####
#         "baltimore"
#         "chicago"
#         "detroit"
        )

FILES=(
#        "hermes"
        "run-hermes.sh"
        "hermes-wings"
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

#for HOST in "${HOSTS[@]}"
#do
#	parallel scp ${HOME_FOLDER}/${FILE} {}:${DEST_FOLDER}/${FILE} ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
#done

#REMOTE_COMMAND="uname -a"
REMOTE_COMMAND="cd ${DEST_FOLDER}; bash run-hermes.sh"
OUTP_FOLDER="/home/${USERNAME}/hermes/results/xput/per-node/"

PASS="${1}"
if [ -z "$PASS" ]
then
      echo "\$PASS is empty! --> sudo pass for remotes is expected to be the first arg"
else
      echo "\$PASS is OK!"
#	  parallel "echo ${PASS} | ssh -tt {} $'${REMOTE_COMMAND}' | cat" ::: sanantonio
#	  parallel "echo ${PASS} | ssh -tt {} $'${REMOTE_COMMAND}' > ${OUTP_FOLDER}/{}.out" ::: sanantonio
      cd ${HOME_FOLDER}
#      echo ${PASS} | sudo -S bash ./run-hermes.sh > ${OUTP_FOLDER}/${LOCAL_HOST}.out &
      echo ${PASS} | bash ./run-hermes.sh > ${OUTP_FOLDER}/${LOCAL_HOST}.out &
	  parallel "echo ${PASS} | ssh -tt {} $'${REMOTE_COMMAND}' > ${OUTP_FOLDER}/{}.out" ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
fi

