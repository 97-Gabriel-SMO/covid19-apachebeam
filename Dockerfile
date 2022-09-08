FROM apache/beam_python3.7_sdk:2.40.0

COPY . /app/

ENTRYPOINT ["python3","main.py"]