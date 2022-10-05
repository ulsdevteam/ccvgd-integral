# Web Preview

<img width="1436" alt="image" src="https://user-images.githubusercontent.com/89871210/188335858-0c64166f-798a-405b-98e3-1f0a5506aeb5.png">

# ccvg_integral
This is the initial deployment test version of the ccvg project
# Deployment Process
You can only use one command to deploy this project by using "docker compose up" or deploy them seperately.
# Environment variables config
Please change the environment variables to the ones required by the user according to the format in env_sample file. For production build, don't forget to change the production flag to true.
## Docker compose
It can be deployed by docker compose up or docker compose up -d.
The docker compose file is:
```
version: "2.3"


services:
  ccvg:

    build:
      context: ./ccvgd-backend
    ports:
      - "80"
    depends_on:
      - db
    restart: always
    links:
      - db
    env_file:
      - ./.env
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.ccvg.rule=Host(`${API_URL}`)"


  db:
    platform: linux/x86_64
    image: mysql:5.7
    container_name: mysqldb
    ports:
      - "3307:3306"
    env_file:
      - ./.env
    volumes:
      - ./db:/docker-entrypoint-initdb.d/:ro


  angular:
      build:
        context: ./ccvgd-frontend
          - PRODUCTION=${PRODUCTION_FLAG}
      ports:
        - "80"
      depends_on:
        - ccvg
        - db
      environment:
        - ./.env
        - API_URL=${API_URL}
        - API_URL_PORT=${API_URL_PORT}
      restart: always
      labels:
        - "traefik.enable=true"
        - "traefik.http.middlewares.angular-redirect-websecure.redirectscheme.scheme=https"
        - "traefik.http.routers.angular.middlewares=angular-redirect-websecure"
        - "traefik.http.routers.angular.entrypoints=web"
        - "traefik.http.routers.angular.rule=Host(`${CCVGD_DOMAIN}`)"
        - "traefik.http.routers.angular-ssl.entrypoints=websecure"
        - "traefik.http.routers.angular-ssl.rule=Host(`${CCVGD_DOMAIN}`)"
        - "traefik.http.routers.angular-ssl.tls=true"



  traefik:
    image: traefik:v2.7
    command:
      - --providers.docker
      - --api.insecure=true
    ports:
      - 8080:8080
      - 80:80
      - 443:443
    volumes:
      - ./traefik/traefik.yaml:/etc/traefik/traefik.yaml:ro
      - /var/run/docker.sock:/var/run/docker.sock
      - ./traefik/tls.yaml:/etc/conf
      - ./certs:/etc/ssl/certs
    labels:
      - "traefik.enable=true"
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
# Go to the home page via angular.docker.localhost
