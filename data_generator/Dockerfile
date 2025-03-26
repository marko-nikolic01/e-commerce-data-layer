FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ /app/

COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

CMD ["sh", "-c", "./run.sh"]
