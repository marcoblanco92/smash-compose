FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

# Disabilita verifica SSL per pip — certificati aziendali non trusted nel container
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

COPY baseline_job.py .

CMD ["python", "-u", "baseline_job.py"]