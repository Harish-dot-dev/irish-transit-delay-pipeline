# 🚌 Irish Transit Delay Pipeline

A real-time end-to-end data engineering pipeline that ingests live Irish public 
transit delay data, processes it through a medallion architecture, and surfaces 
insights via a Power BI dashboard.

## Architecture
TFI GTFS-RT API → Apache Kafka → PySpark Streaming → Bronze Delta Lake
→ Silver Delta Lake
→ Snowflake (dbt Gold models)
→ Power BI Dashboard

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka, Python |
| Processing | PySpark Structured Streaming |
| Storage | Delta Lake (Bronze/Silver), Snowflake (Gold) |
| Transformation | dbt Core |
| Orchestration | Docker Compose |
| Visualisation | Power BI Desktop |
| Infrastructure | Docker, Git |

## Pipeline Layers

**Bronze** — Raw GTFS-RT records ingested from TFI API every 60 seconds via 
Kafka. ~20,000 records per batch. Append-only Delta Lake table.

**Silver** — PySpark batch job cleans Bronze: null filtering, route ID 
normalisation, delay categorisation (on_time / minor / moderate / severe), 
type casting. 2.67M records processed.

**Gold** — dbt Core models running on Snowflake produce 4 aggregated mart tables:
- `mart_delay_by_route` — 344 routes ranked by average delay
- `mart_delay_by_hour` — delay patterns by time of day
- `mart_worst_stops` — top 100 stops by severe delay rate
- `mart_delay_summary` — overall KPI summary

## Key Findings (Live Data — May 2026)

- **2.67M** stop-level delay records captured
- **344** unique routes monitored
- **30.7%** overall delay rate across the TFI network
- Route **2 260 C B** has the highest delay rate at 73.54%
- Stop **8300B103591** (Route 2 D4 C A) has a max delay of 104 minutes
- Delays increase steadily through the day — worst in afternoon hours

## dbt Tests

10/10 data quality tests passing:
- Not null checks on all key columns
- Unique constraints on route and hour aggregations
- Accepted values validation on delay categories

## Project Structure

transit-pipeline/
├── ingestion/
│   ├── kafka_producer.py        # TFI API → Kafka
│   └── silver_to_snowflake.py   # Delta Lake → Snowflake loader
├── processing/
│   ├── spark_streaming.py       # Kafka → Bronze Delta Lake
│   └── silver_transform.py      # Bronze → Silver Delta Lake
├── transit_dbt/
│   ├── models/
│   │   ├── staging/             # stg_transit_cleaned
│   │   └── marts/               # 4 Gold mart models
│   └── macros/
├── dashboard/
│   └── transit_delay_dashboard.pbix
└── docker-compose.yml           # Kafka + Zookeeper

## How to Run

**Prerequisites:** Docker Desktop, Python 3.11+, Java 11, Snowflake account

```bash
# 1. Clone the repo
git clone https://github.com/Harish-dot-dev/irish-transit-delay-pipeline.git
cd irish-transit-delay-pipeline

# 2. Set up environment
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 3. Add credentials
cp .env.example .env
# Fill in TFI_API_KEY and Snowflake credentials

# 4. Start Kafka
docker-compose up -d

# 5. Run the pipeline
python ingestion/kafka_producer.py        # Terminal 1
python processing/spark_streaming.py      # Terminal 2
python processing/silver_transform.py     # After Bronze fills up
python ingestion/silver_to_snowflake.py   # Load to Snowflake

# 6. Run dbt
cd transit_dbt
dbt run
dbt test
```

## Data Source

[Transport for Ireland GTFS-RT API](https://developer.nationaltransport.ie/) — 
free real-time feed covering all Irish bus and rail services.

---

*Built as a portfolio project to demonstrate real-time data engineering 
skills using industry-standard open source tools.*