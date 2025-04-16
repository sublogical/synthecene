



# HOWTO

## Cassandra

* Setup Docker (https://docs.docker.com/engine/install/ubuntu/)
* Run Cassandra (https://cassandra.apache.org/_/quickstart.html)


## Docker

Start cassandra docker instance
```
sudo docker network create cassandra
sudo docker run --rm -d --name cassandra --hostname cassandra --network cassandra cassandra
```


Run CQL client
```
sudo docker run --rm -it --network cassandra nuvo/docker-cqlsh cqlsh cassandra 9042 --cqlversion='3.4.5'
```