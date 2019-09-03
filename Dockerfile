FROM python:3.6-stretch

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY primrose ./primrose
COPY test ./test
RUN mkdir cache

RUN mkdir -p ~/.config/matplotlib && touch ~/.config/matplotlib/matplotlibrc
RUN echo backend: Agg >> ~/.config/matplotlib/matplotlibrc

RUN apt-get update

RUN apt-get install graphviz -y sudo

ENTRYPOINT ["python", "-m", "pytest", "--cov=primrose/", "test/"]