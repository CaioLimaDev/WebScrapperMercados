FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir requests bs4 pandas

CMD [ "python", "app/scrapper.py" ]
