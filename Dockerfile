FROM python:3.6-stretch

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY primrose ./primrose
COPY test ./test
COPY data ./data

RUN mkdir -p ~/.config/matplotlib && touch ~/.config/matplotlib/matplotlibrc
RUN echo backend: Agg >> ~/.config/matplotlib/matplotlibrc

# install r for r reader
RUN echo "deb http://cloud.r-project.org/bin/linux/debian stretch-cran35/" >> /etc/apt/sources.list
RUN apt-key adv --keyserver keys.gnupg.net --recv-key 'E19F5F87128899B192B1A2C2AD5F960A256A04AF'

RUN apt-get update && \
    apt-get install -y r-base r-base-dev && \
    apt-get install graphviz -y sudo

RUN pip install psycopg2 psycopg2_binary pygraphviz rpy2

ENTRYPOINT ["python", "-m", "pytest", "--cov=primrose/", "test/"]