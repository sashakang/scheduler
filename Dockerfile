# syntax=docker/dockerfile:1

FROM python:3.9.5

# # INSTALL MSSQL ODBC DRIVERS
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get -y update
RUN apt-get install -y --no-install-recommends \
        locales \
        apt-transport-https
RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen 
RUN apt-get -y update
RUN apt-get install -y unixodbc-dev
RUN apt-get -y update
RUN ACCEPT_EULA=Y apt-get -y --no-install-recommends install msodbcsql17

RUN mkdir /code
WORKDIR /code

COPY . /code/

RUN python -m pip install -U pip
RUN pip install -r requirements.txt 
COPY . .

ENTRYPOINT [  ]
CMD ["uvicorn", "scheduling_server:app", "--host", "0.0.0.0", "--port", "8000"]