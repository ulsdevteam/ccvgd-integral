# ccvg_integral
This is the initial deployment test version of the ccvg project
# Deployment Process
Since it is a test version, it currently needs to be first into front and back end image files.
The following is the process of creating an image.
## Docker Images
First go into the ccvgd-frontend and ccvgd-backend folders separately.
For frontend,
The dockerfile is:
```
# Use official node image as the base image
FROM node:16 as build

# Add the source code to app
WORKDIR /ccvgd-frontend

COPY . /ccvgd-frontend

# Install all the dependencies
RUN npm install --legacy-peer-deps

# Generate the build of the application
RUN npm run build


# Stage 2: Serve app with nginx server

# Use official nginx image as the base image
FROM nginx:latest

# Expose port 80
EXPOSE 80


WORKDIR /usr/share/nginx/html



COPY --from=build /ccvgd-frontend/dist /usr/share/nginx/html

COPY ./nginx.conf /etc/nginx/nginx.conf
```

```
cd ccvgd-frontend
docker build -t ccvgd-frontend:latest .
```
For backend,
The dockerfile is:
```
FROM python:3.9

ADD . /ccvgd-backend
WORKDIR /ccvgd-backend
ENV PYTHONPATH=/ccvgd-backend

# Install any needed packages specified in requirements_1.txt
COPY requirements.txt /ccvgd-backend
RUN pip3 install -r requirements.txt

# Run app.py when the container launches
COPY app.py /app
CMD python3 app.py runserver -h 0.0.0.0 -p 5050
```
```
cd ccvgd-backend
docker build -t ccvgd-backend:latest .
```
Wait for the image to be generated successfully and then you can deploy it via docker compose.
It can be deployed by docker compose up or docker compose up -d.
The docker compose file is:
```
version: "2.2"


services:
  ccvg:
    image: ccvgd_backend:latest
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
    image: ccvgd-frontend:latest
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
#Go to the home page via http://localhost:4200
