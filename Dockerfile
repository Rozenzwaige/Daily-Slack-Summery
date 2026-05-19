FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY slack_bot/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source
COPY slack_bot/ ./slack_bot/

CMD ["python", "slack_bot/bot.py"]
