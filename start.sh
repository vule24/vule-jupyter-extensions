
docker build -t sparklab-image:latest .
docker stop sparklab-extension-dev
docker rm sparklab-extension-dev
docker run -it \
		--restart unless-stopped \
		--name sparklab-extension-dev \
		-p 1234:8888 \
		-v $(pwd)/vule_magics:/home/jovyan/dev/ \
		-w /home/jovyan/dev/ \
		sparklab-image:latest bash
