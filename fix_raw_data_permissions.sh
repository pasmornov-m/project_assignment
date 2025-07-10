#!/bin/bash

RAW_DATA_DIR="$(pwd)/raw_data"

if [ ! -d "$RAW_DATA_DIR" ]; then
  echo "Ошибка: директория $RAW_DATA_DIR не существует"
  exit 1
fi

USER_GROUP=$(id -g)

sudo chown -R 50000:$USER_GROUP "$RAW_DATA_DIR"

sudo chmod -R 775 "$RAW_DATA_DIR"

echo "Права и владелец успешно установлены для $RAW_DATA_DIR"

# после применить 
# chmod +x fix_raw_data_permissions.sh 