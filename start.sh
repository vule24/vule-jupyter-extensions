
docker build -t spark-lab-test:latest .
docker stop spark-python2-lab
docker rm spark-python2-lab
docker run -d \
		--restart unless-stopped \
		--name spark-python2-lab \
		-p 1234:8888 \
		-p 1235:8889 \
		-v $(pwd):/home/jovyan/work/ \
		spark-lab-test:latest