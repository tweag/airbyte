FROM airbyte/base-python:0.1.0

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /airbyte/integration_code
WORKDIR /airbyte/integration_code

ENTRYPOINT ["python", "main.py"]
