# Adaptive BigQuery Slot Reservation Controller

## Problem Statement
## Why Native Autoscaling Is Not Enough
## Architecture Overview
## Configuration
## Running Locally
## Running with Airflow
## SLA-Based Scaling Logic
## Cost Optimization Strategy

```
Airflow DAG (adapters/)
        ↓
SlotController.run()
        ↓
DecisionEngine
        ↓
Metrics → SLA → Reservation
```