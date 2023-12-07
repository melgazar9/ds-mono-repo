#FROM amd64/python:3.10.12-slim-buster
#
#ARG CI=true
#
#RUN env
#
#ENV PATH="/root/.local/bin:$PATH"
#ENV APP_HOME=/app
#ENV PYTHONUNBUFFERED=1
#WORKDIR $APP_HOME
#COPY . $APP_HOME
#
#RUN apt-get update && rm -rf /var/lib/{apt,dpkg,cache,log}/
#
#RUN pip install -U pip && pip install -r requirements.txt
#
#WORKDIR yfinance/tap-yfinance
#ENV PATH="/root/.local/bin:$PATH"
##RUN pipx ensurepath && poetry install && pipx install "meltano[gcs]==3.1.0" && meltano install
#
#EXPOSE 5000
#CMD ["python", "-V"]
##CMD ["poetry", "run", "python", "host.py"]



FROM amd64/python:3.10.12-slim-buster

ARG CI=true
RUN env
ENV PATH="/root/.local/bin:$PATH"
ENV APP_HOME=/app
ENV PYTHONUNBUFFERED=1
WORKDIR $APP_HOME
COPY . $APP_HOME

RUN apt-get update &&\
    apt-get install -y cron &&\
    apt-get install -y git &&\
    rm -rf /var/lib/{apt,dpkg,cache,log}/

RUN pip install -U pip && pip install pipx==1.2.0 && pip install poetry==1.6.1 && poetry install

WORKDIR yfinance/tap-yfinance
RUN pipx install "meltano[gcs]==3.1.0"
RUN pipx ensurepath
#RUN meltano install

EXPOSE 5000

CMD ["python", "-V"]