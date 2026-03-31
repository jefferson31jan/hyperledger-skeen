#!/bin/bash
# Deploya o JAR do Skeen nos containers frontend já existentes
# Uso: ./deploy_skeen.sh [container1] [container2] ...
# Ex:  ./deploy_skeen.sh bft.frontend.1000 bft.frontend.2000

JAR="dist/BFT-Proxy.jar"
CONFIG="config/skeen.config"

if [ $# -eq 0 ]; then
    echo "Uso: $0 <container1> [container2] ..."
    echo "Ex:  $0 bft.frontend.1000 bft.frontend.2000"
    exit 1
fi

# Verificar se o JAR existe
if [ ! -f "$JAR" ]; then
    echo "JAR não encontrado. Compilando..."
    ant jar
fi

for CONTAINER in "$@"; do
    echo "=== Deployando em $CONTAINER ==="

    # Verificar se o container existe
    if ! docker ps -a --format "{{.Names}}" | grep -q "^${CONTAINER}$"; then
        echo "ERRO: container $CONTAINER não encontrado"
        continue
    fi

    # Copiar JAR novo
    echo "  Copiando BFT-Proxy.jar..."
    docker cp $JAR $CONTAINER:/etc/bftsmart-orderer/bin/BFT-Proxy.jar

    # Copiar skeen.config
    echo "  Copiando skeen.config..."
    docker cp $CONFIG $CONTAINER:/etc/bftsmart-orderer/config/skeen.config

    echo "  Pronto: $CONTAINER"
done

echo ""
echo "Deploy concluído. Reinicie os containers para aplicar."
echo "Ex: docker restart bft.frontend.1000 bft.frontend.2000"
