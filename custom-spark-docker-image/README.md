## Build Image

     docker build -t spark-base-image .
     
#### Build Container
    
    docker build .
    docker run --rm -dit -p 14040:4040 --name spark spark-base-image

Note that we are exposing 4040 port of the docker container as 14040 and will be accessible at `http://localhost:14040`   
docker exec -it mySpark bin/bash
