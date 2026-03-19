

```shell
# Dockerfile 打包
docker build -t grok2api-rs:latest .

# 将镜像转为tar 传输到服务器中
docker save -o grok2api-rs.tar grok2api-rs:latest

scp .\grok2api-rs.tar root@64.32.23.246:/opt/grok2api  

docker load -i grok2api-rs.tar

# 给Linux data文件加权限
mkdir data
 chown -R 10001:10001 ./data   

# 镜像启动
docker run -it -d --name grok-name -p 8000:8000 -v data_docker:/app/data grok2api-rs:latest
# 或者docker-compose 启动
 docker compose up -d
```
