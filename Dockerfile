FROM python
RUN pip install pandas
COPY /src/extract.py /src/
COPY /data/owid-covid-data.csv /data/
CMD ["python", "-u", "/src/extract.py"]
