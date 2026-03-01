# pgmqtt - Machine Page Demo

A demonstration of `pgmqtt`, a PostgreSQL extension that embeds an MQTT 5.0 broker directly inside the database.

![Side-by-side of worker inserting data into DB and dashboard receiving via MQTT](./img/machinepage.webp)

## Architecture

```mermaid
flowchart LR
    %% Definitions
    Worker(fa:fa-cogs Simulation Worker)
    
    subgraph Database Layer
        Postgres[(fa:fa-database PostgreSQL 16)]
        Extension[[pgmqtt Extension]]
    end

    subgraph Presentation Layer
        UI[/fa:fa-desktop Machine Dashboard/]
    end

    %% Connections
    Worker -->|1. Generate Data | Postgres
    Postgres -->|2. CDC Event   | Extension
    Extension -->|3. WebSockets  | UI

    %% Styling
    classDef worker fill:#2A3B4C,stroke:#58A6FF,stroke-width:2px,color:#E6EDF3
    classDef pg fill:#161B22,stroke:#3FB950,stroke-width:2px,color:#E6EDF3
    classDef ext fill:#1F2937,stroke:#D29922,stroke-width:2px,color:#E6EDF3
    classDef ui fill:#0D1117,stroke:#58A6FF,stroke-width:2px,color:#E6EDF3

    class Worker worker
    class Postgres pg
    class Extension ext
    class UI ui
```

## Overview
This project simulates an IIoT/Machine Monitoring environment. A background worker generates mock telemetry data in PostgreSQL, which is then automatically published to MQTT topics via `pgmqtt`'s Change Data Capture (CDC). The web dashboard receives these updates live over WebSockets directly from the database.

## Setup

```bash
cd demos/machinepage
docker-compose up -d
```

open [http://localhost:5173](http://localhost:5173)