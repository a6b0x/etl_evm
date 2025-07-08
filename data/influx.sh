
docker volume create influxdb3_data

docker run -it \
  -v influxdb3_data:/var/lib/influxdb3 \
  influxdb:3-core serve \
  --node-id host01 \
  --object-store file \
  --data-dir /var/lib/influxdb3