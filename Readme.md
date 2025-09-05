# ClearVue Ltd. - Business Intelligence System with Docker

## Project Overview

This project implements a modern Business Intelligence (BI) system for ClearVue Ltd. using Docker containers. The system handles complex reporting requirements aligned with the company's unique financial year cycle, leveraging MongoDB for flexible data storage, Apache Kafka for real-time data processing, and a Node.js application for data ingestion.

## Docker Setup

### Prerequisites

- Docker Desktop installed on your machine
- Docker Compose (usually included with Docker Desktop)
- Power BI Desktop for visualization (installed locally)

### Quick Start

1. **Clone the repository** and navigate to the project directory

2. **Place your Excel files** in the `data/excel` directory

3. **Run the entire stack** with a single command:
```bash
docker-compose up -d
```

4. **Check the status** of all services:
```bash
docker-compose ps
```

5. **View logs** for a specific service:
```bash
docker-compose logs mongodb
docker-compose logs kafka
docker-compose logs data-ingestion
```

6. **Stop the services** when done:
```bash
docker-compose down
```
## Add this directory after installing mongodb]

```
mkdir C:/data/db

```
## Project Structure

```
project-root/
├── docker-compose.yml
├── Dockerfile
├── data/
│   └── excel/           # Place Excel files here
├── scripts/
│   ├── ingest-excel.js  # Data ingestion script
│   └── kafka/
│       ├── producer.js
│       └── consumer.js
├── packages/
│   └── data-ingestion/
│       ├── package.json
│       └── ...          # Node.js application
└── README.md
```

## Docker Services

### 1. MongoDB Service
- Port: 27017 (accessible at localhost:27017)
- Data persistence through Docker volume
- No authentication by default (for development)

### 2. Kafka Service
- ZooKeeper: Port 2181
- Kafka Broker: Port 9092
- Kafka UI: Port 9021 (accessible at http://localhost:9021)

### 3. Data Ingestion Service
- Node.js application that processes Excel files
- Automatically ingests data into MongoDB on startup
- Connects to Kafka for real-time data processing

## Configuration

### Environment Variables

You can customize the setup by creating a `.env` file:

```env
# MongoDB Configuration
MONGO_INITDB_ROOT_USERNAME=admin
MONGO_INITDB_ROOT_PASSWORD=password
MONGO_INITDB_DATABASE=clearvue_db

# Kafka Configuration
KAFKA_BROKER_ID=1
KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181

# Data Ingestion Configuration
DATA_DIR=/app/data/excel
```

### Volumes

The following data is persisted using Docker volumes:
- MongoDB data: `clearvue_mongodb_data`
- Kafka data: `clearvue_kafka_data`
- ZooKeeper data: `clearvue_zookeeper_data`

## Manual Execution

If you want to run the data ingestion manually:

```bash
# Access the data-ingestion container
docker-compose exec data-ingestion bash

# Run the ingestion script manually
node scripts/ingest-excel.js
```

## Connecting Power BI

1. Open Power BI Desktop
2. Click "Get Data" → "More..." → "MongoDB"
3. Enter server: `localhost:27017`
4. Select database: `clearvue_db`
5. Use advanced options to input aggregation pipelines for financial year calculations

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 27017, 9092, 2181, and 9021 are available
2. **Excel files not found**: Place files in the `data/excel` directory
3. **Kafka connection issues**: Check if ZooKeeper is running first

### Useful Commands

```bash
# Check service status
docker-compose ps

# View logs for a specific service
docker-compose logs [service-name]

# Restart a specific service
docker-compose restart [service-name]

# Access MongoDB shell
docker-compose exec mongodb mongosh

# Access Kafka container
docker-compose exec kafka bash

# Force rebuild of containers
docker-compose up -d --build
```

## Development

### Modifying the Data Ingestion Service

1. Make changes to the scripts in the `scripts/` directory
2. Rebuild the container:
```bash
docker-compose up -d --build data-ingestion
```

### Adding New Services

To add new services to the stack:

1. Update the `docker-compose.yml` file
2. Add any necessary Dockerfiles or configuration
3. Run:
```bash
docker-compose up -d [new-service-name]
```

## Production Considerations

For production deployment, you should:

1. Set secure passwords in the `.env` file
2. Enable MongoDB authentication
3. Configure Kafka for better security
4. Set up proper backup strategies for volumes
5. Use a reverse proxy for any web services

## Support

For technical support or questions about this implementation, please check the logs of the specific service experiencing issues or refer to the comprehensive architecture documentation provided with this project.

---

## Files to Create

### 1. Dockerfile
```dockerfile
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
```

### 2. docker-compose.yml
```yaml
version: '3.8'

services:
  mongodb:
    image: mongo:7.0
    container_name: clearvue-mongodb
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db
      - ./data/excel:/data/excel
    environment:
      - MONGO_INITDB_DATABASE=clearvue_db
    networks:
      - clearvue-network

  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: clearvue-zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper_data:/data
      - zookeeper_logs:/datalog
    networks:
      - clearvue-network

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: clearvue-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9021:9021"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
    volumes:
      - kafka_data:/var/lib/kafka/data
    networks:
      - clearvue-network

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: clearvue-kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      - KAFKA_CLUSTERS_0_NAME=local
      - KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:29092
      - KAFKA_CLUSTERS_0_ZOOKEEPER=zookeeper:2181
    networks:
      - clearvue-network

  data-ingestion:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: clearvue-data-ingestion
    depends_on:
      - mongodb
      - kafka
    volumes:
      - ./data/excel:/app/data/excel
      - ./scripts:/app/scripts
    environment:
      - MONGODB_URI=mongodb://mongodb:27017/clearvue_db
      - KAFKA_BROKER=kafka:29092
    networks:
      - clearvue-network
    restart: on-failure

volumes:
  mongodb_data:
  zookeeper_data:
  zookeeper_logs:
  kafka_data:

networks:
  clearvue-network:
    driver: bridge
```

### 3. Updated README.md (as shown above)

This Docker setup provides a complete, self-contained environment for the ClearVue BI system that can be easily deployed and managed.