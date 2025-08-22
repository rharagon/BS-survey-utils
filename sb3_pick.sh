#!/usr/bin/env bash
# sb3_pick.sh

set -euo pipefail

uso() {
  echo "Uso: $(basename "$0") <directorio_origen> [archivo_salida.zip]"
  exit 1
}

[[ "${1:-}" ]] || uso
ORIG="$1"
[[ -d "$ORIG" ]] || { echo "Error: '$ORIG' no es un directorio."; exit 2; }
OUT="${2:-sb3_coleccion_$(date +%Y%m%d_%H%M%S).zip}"

INICIO=$(date +%s)

# Buscar ficheros
FILES=$(find "$ORIG" -type f -name '*.sb3')
COUNT=$(echo "$FILES" | grep -c . || true)

if (( COUNT == 0 )); then
  echo "No se han encontrado archivos .sb3 en '$ORIG'."
  exit 0
fi

# Tamaño total
TOTAL_BYTES=$(echo "$FILES" | xargs wc -c 2>/dev/null | tail -n1 | awk '{print $1}')

# Crear ZIP (rutas relativas)
(cd "$ORIG" && zip -q -r "$OLDPWD/$OUT" . -i '*.sb3')

FIN=$(date +%s)
ZIP_SIZE=$(wc -c <"$OUT")
DUR=$((FIN - INICIO))

# Resumen
echo "----------------------------------------"
echo "Directorio analizado : $ORIG"
echo "Archivo ZIP creado   : $OUT"
echo "Número de .sb3       : $COUNT"
echo "Tamaño total .sb3    : $TOTAL_BYTES bytes"
echo "Tamaño del ZIP       : $ZIP_SIZE bytes"
printf "Duración             : %02d:%02d\n" $((DUR/60)) $((DUR%60))
echo "----------------------------------------"
