FROM jupyter/all-spark-notebook:latest

USER root
RUN echo "${NB_USER} ALL=(ALL:ALL) NOPASSWD:ALL" > /etc/sudoers.d/${NB_USER}

# Install dependencies / Set environment variables as root here

USER ${NB_USER}

# Install dependencies / Set environment variables as user here
RUN conda install -c conda-forge jupyterlab-git -y
# RUN conda create -y -n py27 python=2.7 pyspark
# RUN /opt/conda/envs/py27/bin/python -m pip install 'ipykernel<5.0'

ENV JUPYTER_TOKEN 123

