### Fresh Build (Apple Silicon — native arm64)
- docker build -t dom-sharded -f Docker/Dockerfile_mac .
- docker run -it --name dom-sharded dom-sharded
### Rerun
- docker start -ai dom-sharded
- docker exec -it dom-sharded bash
