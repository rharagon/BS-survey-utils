<#
.SYNOPSIS
    Copia todos los archivos .sb3 de los subdirectorios al directorio de destino especificado.
.DESCRIPTION
    Este script busca recursivamente todos los archivos con extensión .sb3 a partir del directorio de ejecución
    y los copia a un directorio de destino proporcionado por el usuario.
.PARAMETER Destino
    Ruta del directorio de destino donde se copiarán los archivos .sb3.
#>

param (
    [Parameter(Mandatory=$true)]
    [string]$Destino
)

# Verificar si el directorio de destino existe, si no, crearlo
if (-not (Test-Path -Path $Destino)) {
    New-Item -ItemType Directory -Path $Destino -Force | Out-Null
    Write-Host "Directorio de destino creado: $Destino"
}

# Obtener todos los archivos .sb3 en el directorio actual y subdirectorios
$archivosSB3 = Get-ChildItem -Path . -Recurse -Filter "*.sb3" -File

if ($archivosSB3.Count -eq 0) {
    Write-Host "No se encontraron archivos .sb3 en el directorio actual ni en sus subdirectorios."
    exit
}

# Copiar cada archivo al directorio de destino
foreach ($archivo in $archivosSB3) {
    $destinoCompleto = Join-Path -Path $Destino -ChildPath $archivo.Name
    Copy-Item -Path $archivo.FullName -Destination $destinoCompleto -Force
    Write-Host "Copiado: $($archivo.FullName) -> $destinoCompleto"
}

Write-Host "Todos los archivos .sb3 han sido copiados a $Destino."
