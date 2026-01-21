# BigQuery Dynamic Slot Reservation

## Problem Definition

### Bigquery's Cost Inefficiency Due to Autoscaling Cooldown (“Buffer Time”)

A practical limitation of native BigQuery autoscaling is its conservative scale-down behavior. After a burst of heavy queries, reservation slots ramp up quickly, but they scale down slowly due to an internal cooldown period. During this time, unused slots remain allocated and billed.

For workloads with short-lived spikes, periodic batch jobs, or bursty analytical queries, this behavior can lead to systematic over-allocation and inflated costs.

### Real-world Impact Observed

In a previous production environment, this pattern caused:
- Slot reservations to remain elevated long after peak workloads completed
- Daily cost spikes during predictable batch windows
- Limited visibility or control over when scale-down would occur

By monitoring reservation slot usage via Google Cloud Monitoring, we observed that:
- Even after all query jobs had finished, reservation slots remained high for a period of idleness
- This idle allocation directly increased costs, especially at scale — for a company managing hundreds of PBs of analytical data, the financial impact was substantial

### Proposed solution
The pipelines in this repository focus on policy-driven slot controller that:
- Slot capacity was actively brought back down once SLA conditions stabilized
- Scale-down decisions became time-aware and workload-aware

### Measured outcome (pilot phase):
- 5–15% reduction in BigQuery slot reservation costs (observed across multiple projects in Google Cloud)
- Measured over ~1 week of controlled monitoring
- No SLA regressions observed during the pilot

### How Does this mitigate the autoscaling buffer inefficiency
- Enforcing explicit minimum slots only when needed
- Actively reducing max autoscale limits outside peak windows
- Using SLA health as a signal to stop over-provisioning
- Avoiding “set-and-forget” capacity ceilings

## Architecture Overview

This project implements a policy-driven BigQuery slot autoscaling system.
The architecture separates orchestration, decision logic, SLA policy,
metrics collection, and infrastructure APIs for clarity, testability,
and portability across schedulers.

### Architecture Flow
```mermaid
flowchart TD
    %% Entry point
    A[Airflow DAG<br/>adapters/airflow_dag.py] --> B[SlotController<br/>core/controller.py]

    %% Controller orchestration
    B --> C[DecisionEngine<br/>core/decision_engine.py]

    %% Metrics collection
    C --> D[BigQueryJobMetricsCollector<br/>core/metrics.py]
    D --> E[(BigQuery INFORMATION_SCHEMA)]
    E --> D

    %% SLA evaluation
    C --> F[SLAPolicy<br/>core/sla_policy.py]

    %% Decision outcomes
    F -->|SLA Healthy| G[No Action]
    F -->|SLA Breach| H[ReservationManager]

    %% Slot management
    H --> I[BigQuerySlotReservation<br/>core/reservation.py]
    I --> J[(BigQuery Reservation API)]

    %% Config & SQL
    K[configs/reservation_slot_configs.json] --> B
    L[queries/jobs_sla_metrics.sql] --> D
```

### Decision Engine Sub-Flow
```mermaid
sequenceDiagram
    participant DAG as Airflow DAG
    participant DE as DecisionEngine
    participant MC as MetricsCollector
    participant SLA as SLAPolicy
    participant RM as ReservationManager
    participant BQ as BigQuery API

    DAG->>DE: run(execution_time)
    DE->>MC: collect(monitoring_time)
    MC->>BQ: INFORMATION_SCHEMA query
    BQ-->>MC: aggregated metrics
    MC-->>DE: metrics

    DE->>SLA: evaluate(metrics)
    SLA-->>DE: SLAEvaluationResult

    alt SLA Healthy
        DE->>DE: enforce min slots if needed
    else SLA Breach
        DE->>RM: set_slots / add_slots
        RM->>BQ: update reservation
    end
```

## Design Rationale

The configurations is split into three logical layers to support reusability and operational safety:
1. Metadata
2. Slot Profiles
3. Time-Based Mapping / Schedules

### 1. Metadata (Environment Binidng)
```
"metadata": {
  "project_id": "...",
  "reservation_id": "...",
  "location": "asia-southeast2"
}
```
This layer keeps environment-specific details separate from scaling logic. The purpose is to enable reusability of the same logic policy across different projects/regions/reservations. Thus, allowing the pipelines to be promoted between environments (dev to prod) with minimal changes.

### 2. Slot Profiles (Reussable Capacity Policies)
```
"reservation_slot_profiles": {
  "low":    { "min": 1500, "max": 2000, "increment": 100 },
  "medium": { "min": 2500, "max": 3000, "increment": 100 },
  "high":   { "min": 3500, "max": 4000, "increment": 100 }
}
```
Slot profiles represent abstract slot capacities, the purpose is to avoid duplicating slot numbers across multiple time windows and to allow easy tuning of capacity behavior without changing logic. If a profile needs adjustment (e.g., increasing peak max slots), it can be done once and applied everywhere. It also makes scaling decision policies explicit, and not just ad-hoc based.

### 3. Time-Based Mapping (Peak Hour Scheduling)
```
"reservation_time_mapping": {
  "0": {
    "8":  { "0": "high", "30": "high" },
    "18": { "0": "low",  "30": "low"  }
  }
}
```
This layer enables predictable baseline capacity during known peak hours. The key principle for this time-based configurations is to define the expected behavior, while SLA signals correct deviations in real-time. The configuration maps weekday → hour → 30-minute window → slot profile, and it uses Python’s `day_of_week` semantics (0 = Monday).

## Near real-time SLA Metric feedback 
In parallel, the system actively monitors BigQuery workload health by querying `INFORMATION_SCHEMA.JOBS` at a fixed interval (typically 3 or 5 minutes). The controller evaluates SLA health using pending job counts, queueing time percentiles, max running job durations and error job ratios, these metrics are directly tied to user experience, making them ideal inputs for scaling decisions. At each evaluation cycle, the controller collect recent job metrics from the previous window, evaluates SLA health (queueing time, pending jobs, errors, long-running queries), and if needed, adjusts reservation capacity by incrementing the slot by 50 or 100. This way of scaling prevents over-provisioning from short-lived spikes and reduces the risk of cost explosions caused by noisy or transient workloads.

## Window Reset Behavior
To ensure that long cooldown/buffer periods from BigQuery autoscaling are effectively bypassed, each 30-minute window acts as a natural reset point. When the controller transitions into a new window, slot capacity is re-aligned with the configured baseline for that window and any temporary scale-ups from the previous window do not automatically carry over. The system starts from a clean, policy-defined state and cost returns to expected levels once demand subsides.

## Considerations & Limitations

### Randomness of Workloads

Query spikes can happen at unpredictable times (e.g., national holidays, ad-hoc analysis, migrations, emergency debugging, etc.).
This randomness makes purely predictive or ML-based slot forecasting unreliable.

### Policy-Driven Approach Only

The pipeline enforces decisions based on SLA signals and time-of-day heuristics, not predictions.
It actively scales down after periods of low usage and avoids over-provisioning, but cannot preemptively predict rare spikes.

### Scope of Slot Adjustment

Currently, the system only adjusts max autoscaling slots of a single reservation.
Multi-reservation or cross-project coordination is not handled yet.

### Reliance on Metrics Availability

Accurate decision-making depends on timely and correct metrics from BigQuery.
Missing or delayed metrics may trigger conservative slot increases.

### Buffer Time Mitigation, Not Elimination

While this system reduces unnecessary costs, BigQuery’s internal scaling buffer still exists.
The system works around it by using SLA health and explicit minimum slots instead of trying to remove the internal cooldown entirely.

### No SLA Regression Observed (Pilot Phase)

In our pilot monitoring, cost savings were achieved without violating SLAs.
Continuous monitoring is recommended if workloads or usage patterns change significantly.
