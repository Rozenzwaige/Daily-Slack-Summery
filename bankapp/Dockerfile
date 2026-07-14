FROM python:3.12-slim

WORKDIR /app

COPY requirements-bot.txt .
RUN pip install --no-cache-dir -r requirements-bot.txt

COPY webhook.py sheets.py whatsapp.py ./

ENV PORT=8080
EXPOSE 8080

# 1 worker keeps the in-memory conversation history consistent
CMD ["gunicorn", "-b", "0.0.0.0:8080", "-w", "1", "-t", "120", "webhook:app"]
