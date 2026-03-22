# Salesforce-Databricks Connector

> Real-Time Data Sync between Salesforce and Databricks

## Architecture

![Architecture Diagram](architecture/architecture-diagram.png)

<!-- TODO: Replace with actual architecture diagram from excalidraw.com -->

## Overview

A production-grade connector that enables real-time bidirectional data synchronization between Salesforce and Databricks. Uses Salesforce Change Data Capture (CDC) events streamed through a scalable pipeline to keep Databricks Delta Lake tables in sync with Salesforce objects, enabling real-time analytics and AI/ML workloads on CRM data.

### Key Features

- **Real-Time CDC Streaming**: Captures Salesforce changes via Change Data Capture events
- **Delta Lake Integration**: Upserts into Databricks Delta tables with ACID guarantees
- **Bidirectional Sync**: Write-back capability from Databricks to Salesforce via REST API
- **Schema Evolution**: Automatic handling of Salesforce field additions/changes
- **Monitoring**: Built-in health checks, retry logic, and observability dashboards

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Source | Salesforce (Change Data Capture) |
| Streaming | Apache Kafka / Salesforce Pub/Sub API |
| Processing | Databricks (PySpark / Delta Live Tables) |
| Storage | Delta Lake |
| Orchestration | Databricks Workflows |
| API | Salesforce REST/Bulk API |
| SDK | Databricks SDK, Simple Salesforce |

## Setup

### Prerequisites

- Python 3.11+
- Databricks workspace with Unity Catalog
- Salesforce org with CDC enabled
- Kafka cluster (optional, for high-throughput)

### Installation

```bash
git clone https://github.com/gyaneshwarmalba/salesforce-databricks-connector.git
cd salesforce-databricks-connector
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # Fill in your credentials
```

### Running

```bash
python -m app.main
```

## Demo

<!-- TODO: Add demo GIF/video -->

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
