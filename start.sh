
# docker build -t spark-python2-lab:latest .
docker stop spark-python2-lab
docker rm spark-python2-lab
docker run -d \
		--restart unless-stopped \
		--name spark-python2-lab \
		-p 1234:8888 \
		-v $(pwd):/home/jovyan/work/extension \
		spark-python2-lab:latest