# Use any Python version you need; 3.11-slim is a sensible default.
FROM python:3.11-slim



WORKDIR /app


COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY . /app



# CMD ["uvicorn", "webhook:app", "--host", "0.0.0.0", "--port", "5000"]


