# syntax=docker/dockerfile:1

FROM python:3.11

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

# # Create and activate virtual environment
RUN python -m venv /workspaces/code/.venv
ENV PATH="/workspaces/code/.venv/bin:$PATH"

# # Install dependencies from requirements.txt
COPY ./requirements.txt /workspaces/code/requirements.txt
RUN pip install --no-cache-dir -r /workspaces/code/requirements.txt

# # Copy the rest of your code
COPY . /workspaces/code/

# COPY ./requirements.txt /code
# RUN python -m pip install -U pip
# RUN python -m pip install -r requirements.txt 

COPY . /code/

# After setting up your workspace
RUN ln -s /container-credentials /workspaces/code/../.credentials

# what does this line do? Is it really necessary?
# COPY . .    

ENTRYPOINT [  ]
CMD ["uvicorn", "scheduling_server:app", "--host", "0.0.0.0", "--port", "8000"]