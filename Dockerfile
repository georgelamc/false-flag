FROM python
RUN pip install pandas psycopg2 sqlalchemy
COPY /src/extract.py /src/
COPY /data/owid-covid-data.csv /data/
CMD ["python", "-u", "/src/extract.py"]
