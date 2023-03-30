FROM amd64/python:3.9.16-slim-buster

ARG CI=true

# Receive input from --build arg
ARG APP_ENV
ENV APP_ENV=$APP_ENV
RUN echo build and runtime APP_ENV: $APP_ENV
RUN env

ENV APP_HOME=/app
ENV PYTHONUNBUFFERED=1
RUN env

WORKDIR $APP_HOME
COPY . $APP_HOME

WORKDIR $APP_HOME/projects/financial_elt

RUN apt-get update &&\
    apt-get install -y cron &&\
    apt-get install -y python3-dev default-libmysqlclient-dev build-essential &&\
    rm -rf /var/lib/{apt,dpkg,cache,log}/

RUN pip install -U pip &&\
    pip install -r requirements.txt

EXPOSE 5000

CMD ["python", "host_financial_elt.py"]