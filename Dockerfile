FROM python:3.10.14

WORKDIR /app

COPY . /app

EXPOSE 80

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "daily_update.py"]