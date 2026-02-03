#!/bin/bash

set -a
. ./.env
set +a

sudo mkdir "$TRUENAS_MOUNT"
sudo mount -t cifs "$TRUENAS_IP" "$TRUENAS_MOUNT" \
  -o "username=$TRUENAS_USER,password=$TRUENAS_PASS,domain=$TRUENAS_DOMAIN,uid=1000,gid=1000,ro,noserverino,vers=3.0"

if [ $? -eq 0 ]; then
    echo "CIFS share montado en $TRUENAS_MOUNT correctamente ✅"
else
    echo "Error montando el share ❌"
fi


