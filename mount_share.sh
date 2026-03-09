#!/bin/bash

set -a
. ./.env
set +a

# Comprobar si el directorio ya está montado
if mountpoint -q "$TRUENAS_MOUNT"; then
    echo "El share ya está montado en $TRUENAS_MOUNT ✅"
else
    echo "Montando el share CIFS en $TRUENAS_MOUNT..."
    
    # Crear el directorio si no existe
    sudo mkdir -p "$TRUENAS_MOUNT"
    
    # Montar el share
    sudo mount -t cifs "$TRUENAS_IP" "$TRUENAS_MOUNT" \
      -o "username=$TRUENAS_USER,password=$TRUENAS_PASS,domain=$TRUENAS_DOMAIN,uid=1000,gid=1000,vers=3.0,serverino,cache=loose,rsize=1048576,wsize=1048576,actimeo=60"
    
    if [ $? -eq 0 ]; then
        echo "CIFS share montado en $TRUENAS_MOUNT correctamente ✅"
    else
        echo "Error al montar el share. Verifica credenciales y conectividad 🛠️"
    fi
fi



