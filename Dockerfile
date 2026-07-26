FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render dynamically sets PORT environment variable
EXPOSE 8765

CMD ["python", "server.py"]
