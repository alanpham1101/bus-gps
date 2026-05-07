docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic hcmc_bus_gps \
  --partitions 10 \
  --replication-factor 1
