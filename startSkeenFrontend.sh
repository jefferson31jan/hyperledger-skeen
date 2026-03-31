#!/bin/bash
# Frontend com Skeen integrado
# Uso: ./startSkeenFrontend.sh <frontendID> <poolSize> <sendPort>
# Ex:  ./startSkeenFrontend.sh 1000 20 9999

export SKEEN_SHARD_ID=${SKEEN_SHARD_ID:-ShardA}
export FRONTEND_CONFIG_DIR=${FRONTEND_CONFIG_DIR:-config/}

echo "Iniciando frontend Skeen — shard=$SKEEN_SHARD_ID"

java -DFRONTEND_CONFIG_DIR=$FRONTEND_CONFIG_DIR \
     -cp dist/BFT-Proxy.jar:lib/* \
     bft.BFTProxy $@
