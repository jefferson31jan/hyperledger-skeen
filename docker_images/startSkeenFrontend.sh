#!/bin/bash
parse_yaml() {
   local prefix=$2
   local s='[[:space:]]*' w='[a-zA-Z0-9_]*' fs=$(echo @|tr @ '\034')
   sed -ne "s|^\($s\)\($w\)$s:$s\"\(.*\)\"$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p" $1 | sed 's/\$/\\\$/g' |
   awk -F$fs '{
      indent = length($1)/4;
      vname[indent] = $2;
      for (i in vname) {if (i > indent) {delete vname[i]}}
      if (length($3) > 0) {
         vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
         printf("%s%s%s=\"%s\"\n", "'$prefix'",vn, $2, $3);
      }
   }'
}
function main () {
        eval $(parse_yaml /etc/hyperledger/fabric/orderer.yaml "orderer_")
        # Passa SKEEN_SHARD_ID para o processo Java
        SKEEN_SHARD_ID=$SKEEN_SHARD_ID \
        java -cp bin/orderingservice.jar:lib/* bft.BFTProxy $1 $orderer_BFTsmart_ConnectionPoolSize $orderer_BFTsmart_RecvPort &
        sleep 2
        orderer start
}
main $@
