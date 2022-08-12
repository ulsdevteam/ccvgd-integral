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
    # image: ccvgd_backend:latest
    build: ./ccvgd-backend
    ports:
      - "5050:5050"
    depends_on:
      - db
    restart: always
    links:
      - db 
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
    # image: ccvgd-frontend:latest
    build: ./ccvgd-frontend
    ports:
      - "4200:80"
    depends_on:
      - ccvg
      - db
    restart: always



```
```
docker compose up
```
# Go to the home page via http://localhost:4200
