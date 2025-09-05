FROM node:18-alpine

WORKDIR /app

# Copy package files and install dependencies
COPY packages/data-ingestion/package*.json ./
RUN npm install

# Copy application code
COPY packages/data-ingestion/ ./
COPY scripts/ ./scripts/
COPY data/ ./data/

# Create directory for Excel files
RUN mkdir -p /app/data/excel

# Expose port (if applicable)
EXPOSE 3000

# Command to run on container start
CMD ["node", "scripts/ingest-excel.js"]
