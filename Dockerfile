###### READ ######
# Currently there are no projects that reference this root Dockerfile.
# It is being used as a placeholder for projects that require a light weight common set of packages.

FROM amd64/python:3.9.15-slim-buster

ARG CI=true

# Receive input from --build-arg
ARG APP_ENV
ENV APP_ENV=$APP_ENV
RUN echo build and runtime APP_ENV: $APP_ENV
RUN env

ENV APP_HOME=/app

ENV PYTHONUNBUFFERED=1

WORKDIR $APP_HOME

RUN apt-get update &&\
    apt-get install -y cron &&\
    rm -rf /var/lib/{apt,dpkg,cache,log}/

COPY . $APP_HOME

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

EXPOSE 5000

CMD ["python", "host_projects.py"]
