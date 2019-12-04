#!/usr/bin/env bash

source run.sh

blue "Running hades"

sudo LD_LIBRARY_PATH=/usr/local/lib/ -E \
	./hades                             \
	--machine-id ${NODE_ID}             \
	--dev-name "mlx5_0"                 \
	2>&1
