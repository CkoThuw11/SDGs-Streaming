FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install psycopg2-binary

# Copy script
COPY generator.py .

# Run script
CMD ["python", "generator.py"]
