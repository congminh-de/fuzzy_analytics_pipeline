FROM python:3.11-slim


ENV DEBIAN_FRONTEND=noninteractive


WORKDIR /opt/dagster/app


RUN apt-get update && apt-get install -y \
    build-essential \
    libmariadb-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*


COPY requirements.txt .


RUN pip install uv && python -m uv pip install --system --no-cache -r requirements.txt 


COPY . .


ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME


EXPOSE 3000


CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]






















