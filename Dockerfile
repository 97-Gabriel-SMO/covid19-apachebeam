FROM python:3.10.7-bullseye
ENV PYTHONUNBUFFERED 1
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt