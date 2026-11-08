#!/usr/bin/env sh
# S3DynamoDBLogStore de Delta 3.3.3 no deja configurar el endpoint de DynamoDB: siempre usa el de la
# región de AWS, así que no puede hablar con DynamoDB Local. Este script toma la fuente oficial
# (el JAR -sources verificado por SHA-1), le aplica ddb-endpoint.patch (agrega la opción
# `ddb.endpoint`), la compila y reemplaza la clase dentro del JAR oficial.
# En AWS no hace falta: se usa el JAR original sin `ddb.endpoint`.
#
# Uso: construir_logstore.sh DIR_RUNTIME DIR_COMPILACION
set -eu

AQUI=$(cd "$(dirname "$0")" && pwd)
RUNTIME=$(cd "$1" && pwd)
COMPILACION=$(cd "$2" && pwd)
DELTA=3.3.3
ORIGINAL="$RUNTIME/delta-storage-s3-dynamodb-$DELTA.jar"
TRABAJO=$(mktemp -d)

cd "$TRABAJO"
jar xf "$COMPILACION/delta-storage-s3-dynamodb-$DELTA-sources.jar" io/delta/storage/S3DynamoDBLogStore.java
patch -p1 < "$AQUI/ddb-endpoint.patch"
mkdir clases
javac --release 8 -nowarn -Xlint:-options -d clases \
  -cp "$RUNTIME/delta-storage-$DELTA.jar:$ORIGINAL:$RUNTIME/aws-java-sdk-bundle-1.12.262.jar:$COMPILACION/hadoop-client-api-3.3.4.jar:$COMPILACION/slf4j-api-2.0.7.jar" \
  io/delta/storage/S3DynamoDBLogStore.java
(cd clases && jar uf "$ORIGINAL" io/delta/storage/S3DynamoDBLogStore*.class)
mv "$ORIGINAL" "$RUNTIME/delta-storage-s3-dynamodb-$DELTA-breb.jar"
rm -rf "$TRABAJO"
echo "ok delta-storage-s3-dynamodb-$DELTA-breb.jar (S3DynamoDBLogStore con ddb.endpoint)"
