#!/usr/bin/env bash

FILES_TO_CPY=(
        "hosts.sh"
        "run.sh"
        "run-hermesKV.sh"
        "hermesKV"
        "run-rCRAQ.sh"
        "rCRAQ"
#        "hades"
#        "run-hades.sh"
      )

EXEC_FOLDER="${HOME}/hermes/exec"

cd $EXEC_FOLDER
# get Hosts
source ../exec/hosts.sh
make clean; make
cd -

for FILE in "${FILES_TO_CPY[@]}"
do
	parallel scp ${EXEC_FOLDER}/${FILE} {}:${EXEC_FOLDER}/${FILE} ::: $(echo ${REMOTE_HOSTS[@]})
	echo "${FILE} copied to {${REMOTE_HOSTS[@]}}"
done

