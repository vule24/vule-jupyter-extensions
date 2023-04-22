# FROM --platform=linux/amd64 jupyter/all-spark-notebook:latest
FROM jupyter/all-spark-notebook:latest

USER root
RUN echo "${NB_USER} ALL=(ALL:ALL) NOPASSWD:ALL" > /etc/sudoers.d/${NB_USER}

# Install dependencies / Set environment variables as root here

USER ${NB_USER}

# Install dependencies / Set environment variables as user here
# RUN conda create -n py27 python=2.7
# RUN /opt/conda/envs/py27/bin/python -m pip install 'ipykernel<5.0'
# RUN /opt/conda/envs/py27/bin/python -m ipykernel install --user --name p27 --display-name "Python 2"

ENV JUPYTER_TOKEN 123

