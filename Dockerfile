FROM python
RUN pip install confluent-kafka pandas psycopg2 sqlalchemy
COPY /src/extract.py /src/
COPY /data/nearest-earth-objects.csv /data/
CMD ["python", "-u", "/src/extract.py"]
