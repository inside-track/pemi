FROM python:3.6
ENV PYTHONUNBUFFERED 1

RUN apt-get update -qq && apt-get install --no-install-recommends -y \
  graphviz \
  postgresql-client \
  bash-completion \
  && rm -rf /var/lib/apt/lists/*
RUN echo 'source /usr/share/bash-completion/bash_completion' >> /etc/bash.bashrc

# Install tini
ARG TINI_VERSION
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN echo 'export HISTFILE=$HOME/.bash_history/history' >> $HOME/.bashrc

WORKDIR /app
COPY ./requirements.txt requirements.txt
RUN pip install pip-tools invoke && rm -rf /root/.cache
RUN pip install -r requirements.txt && rm -rf /root/.cache

# Configure Jupyterlab
RUN jupyter serverextension enable --py jupyterlab --sys-prefix
ADD docker/jupyter_notebook_config.py /root/.jupyter/jupyter_notebook_config.py



ENTRYPOINT ["/tini", "--"]
#CMD jupyter lab . --ip=0.0.0.0 --allow-root --no-browser
