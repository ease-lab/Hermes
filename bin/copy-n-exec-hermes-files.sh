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
RESULT_FOLDER="/home/${USERNAME}/hermes-exec/results/xput/per-node/"
RESULT_OUT_FOLDER="/home/${USERNAME}/hermes/results/xput/per-node/"
RESULT_OUT_FOLDER_MERGE="/home/${USERNAME}/hermes/results/xput/all-nodes/"

cd $MAKE_FOLDER
make clean
make
cd -

for FILE in "${FILES[@]}"
do
	parallel scp ${HOME_FOLDER}/${FILE} {}:${DEST_FOLDER}/${FILE} ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
	echo "${FILE} copied to {${HOSTS[@]/$LOCAL_HOST}}"
done

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

#	  parallel "echo ${PASS} | ssh -tt {} $'${REMOTE_COMMAND}' > ${OUTP_FOLDER}/{}.out" ::: $(echo ${HOSTS[@]/$LOCAL_HOST}) &
#      echo ${PASS} | ./run-hermes.sh > ${OUTP_FOLDER}/${LOCAL_HOST}.out

      # Execute locally and remotely
      echo ${PASS} | ./run-hermes.sh &
	  parallel "echo ${PASS} | ssh -tt {} $'${REMOTE_COMMAND}'" ::: $(echo ${HOSTS[@]/$LOCAL_HOST})

      # Gather remote files
	  parallel "scp {}:${RESULT_FOLDER}* ${RESULT_OUT_FOLDER} " ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
	  echo "Files copied from: {${HOSTS[@]/$LOCAL_HOST}}"

	  # group all files
      ls ${RESULT_OUT_FOLDER} | awk -F '-' '!x[$1]++{print $1}' | while read -r line; do
          #// Create an intermediate file print the 3rd line for all files with the same prefix to the same file
          awk 'FNR==3 {print $0}' ${RESULT_OUT_FOLDER}/$line* > ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt
          #   Sum up the xPut of the (3rd iteration) from every node to create the final file
          awk -F ':' '{sum += $2} END {print sum}' ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt > ${RESULT_OUT_FOLDER_MERGE}/$line.txt
          rm -rf  ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt
      done

	  cd -
fi


