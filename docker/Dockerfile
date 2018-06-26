FROM ubuntu:14.04

# Miniconda (w/ Python 3.6)
RUN apt-get update && apt-get install -y\
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV CONDA_DIR /opt/conda
ENV PATH $CONDA_DIR/bin:$PATH
ENV MINICONDA_VERSION 4.5.4
RUN cd /tmp && \
    mkdir -p $CONDA_DIR && \
    wget --quiet https://repo.continuum.io/miniconda/Miniconda3-${MINICONDA_VERSION}-Linux-x86_64.sh && \
    echo "a946ea1d0c4a642ddf0c3a26a18bb16d *Miniconda3-${MINICONDA_VERSION}-Linux-x86_64.sh" | md5sum -c - && \
    /bin/bash Miniconda3-${MINICONDA_VERSION}-Linux-x86_64.sh -f -b -p $CONDA_DIR && \
    rm Miniconda3-${MINICONDA_VERSION}-Linux-x86_64.sh && \
    $CONDA_DIR/bin/conda config --system --prepend channels conda-forge && \
    $CONDA_DIR/bin/conda config --system --set auto_update_conda false && \
    $CONDA_DIR/bin/conda config --system --set show_channel_urls true && \
    $CONDA_DIR/bin/conda update --all && \
    conda clean -tipsy

# JAVA 8
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y  software-properties-common \
    && add-apt-repository ppa:webupd8team/java -y \
    && apt-get update \
    && echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections \
    && apt-get install -y oracle-java8-installer \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# App-specific packages
RUN apt-get update && apt-get install -y \
    graphviz \
    postgresql-client \
    bash-completion \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Tini
## See https://github.com/krallin/tini/releases for latest release number
ENV TINI_VERSION=v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

# Conveniences
RUN echo 'source /usr/share/bash-completion/bash_completion' >> /etc/bash.bashrc
RUN echo 'export HISTFILE=$HOME/.bash_history/history' >> $HOME/.bashrc

# App
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV PYTHONUNBUFFERED 1
WORKDIR /app
COPY ./requirements/all.txt requirements/all.txt
RUN pip install --upgrade pip
RUN pip install pip-tools invoke && rm -rf /root/.cache
RUN pip install -r requirements/all.txt && rm -rf /root/.cache

# Configure Jupyterlab
ENV PYTHONPATH /app
RUN jupyter serverextension enable --py jupyterlab --sys-prefix
ADD docker/jupyter_notebook_config.py /root/.jupyter/jupyter_notebook_config.py

ENTRYPOINT ["/tini", "--"]
