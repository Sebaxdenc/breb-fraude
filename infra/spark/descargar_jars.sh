#!/usr/bin/env sh
# Descarga de Maven Central los JARs de jars.txt y verifica el SHA-1 de cada uno.
# Uso: descargar_jars.sh jars.txt DIR_RUNTIME DIR_COMPILACION
set -eu

LISTA="$1"
RUNTIME="$2"
COMPILACION="$3"
MAVEN="${MAVEN_URL:-https://repo1.maven.org/maven2}"
mkdir -p "$RUNTIME" "$COMPILACION"

grep -vE '^[[:space:]]*(#|$)' "$LISTA" | while read -r coord sha1 uso; do
  grupo=$(echo "$coord" | cut -d: -f1)
  artefacto=$(echo "$coord" | cut -d: -f2)
  version=$(echo "$coord" | cut -d: -f3)
  clasificador=$(echo "$coord" | cut -d: -f4)
  archivo="$artefacto-$version${clasificador:+-$clasificador}.jar"
  destino="$RUNTIME"
  [ "${uso:-}" = "compilacion" ] && destino="$COMPILACION"
  url="$MAVEN/$(echo "$grupo" | tr . /)/$artefacto/$version/$archivo"
  curl -fsSL --retry 3 -o "$destino/$archivo" "$url"
  echo "$sha1  $destino/$archivo" | sha1sum -c - >/dev/null || { echo "SHA-1 distinto: $archivo" >&2; exit 1; }
  echo "ok $archivo"
done
