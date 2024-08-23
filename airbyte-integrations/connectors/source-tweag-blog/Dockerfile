FROM airbyte/python-connector-base:1.1.0

# Install git, curl, and Node.js
RUN apt-get update && \
    apt-get install -y git curl && \
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs

# Install global npm packages, including Gatsby CLI
# RUN npm install -g gatsby-cli

# Copy the application code
COPY . /airbyte/integration_code

# Install Python dependencies
WORKDIR /airbyte/integration_code
RUN pip install .

# The entrypoint and default env vars are already set in the base image
ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
