FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Streamlit app
COPY viz_app.py .

# Expose Streamlit default port
EXPOSE 8501

ENV PYTHONUNBUFFERED=1

# Run Streamlit with proper configuration
CMD ["streamlit", "run", "viz_app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
