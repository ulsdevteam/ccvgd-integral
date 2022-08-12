# ccvg_integral
This is the initial deployment test version of the ccvg project
# Deployment Process
You can only use one command to deploy this project by using "docker compose up" or deploy them seperately.

## Docker compose
It can be deployed by docker compose up or docker compose up -d.
The docker compose file is:
```
version: "2.2"


services:
  ccvg:

    build: ./ccvgd-backend
    ports:
      - "5050:5050"
    depends_on:
      - db
    restart: always
    links:
      - db 
    labels:
      - "traefik.http.routers.ccvg.rule=Host(`ccvg.docker.localhost`)"


  db:

    platform: linux/x86_64
    image: mysql:5.7
    container_name: local1
    ports:
      - "3307:3306"
    environment:
      - MYSQL_ROOT_PASSWORD=123456
      - MYSQL_DATABASE=ccvg_5_18
    volumes:
      - ./db:/docker-entrypoint-initdb.d/:ro
  angular:

    build: ./ccvgd-frontend
    ports:
      - "4200:80"
    depends_on:
      - ccvg
      - db
    restart: always
    labels:
       - "traefik.http.routers.angular.rule=Host(`angular.docker.localhost`)"
  
  traefik:
    image: traefik:v2.7
    command:
      - --providers.docker
      - --api.insecure=true
    ports:
      - "8080:8080"
      - "80:80"
      - "443:443"
    volumes:
      - ./traefik/traefik.toml:/etc/traefik/traefik.toml:ro
      - /var/run/docker.sock:/var/run/docker.sock 
    labels:
      - "traefik.http.routers.api.rule=Host(`traefik.docker.localhost`)"
      - "traefik.http.routers.api.service=api@internal"
    depends_on:
      - db
      - ccvg
      - angular
    restart: always


```
```
docker compose up
```
# Go to the home page via http://localhost:4200
