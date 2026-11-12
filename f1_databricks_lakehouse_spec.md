# F1 Databricks Lakehouse & Lap Simulation — Agentic Engineering Specification

This document serves as the authoritative technical specification and execution blueprint for building an end-to-end Formula 1 Data Engineering Lakehouse on **Databricks**. It is structured for use in an agentic AI development environment (e.g., `AGENTS.md`, `SPEC.md`, or project context) so an autonomous coding agent can scaffold, implement, test, and deploy the pipeline.

---

## 1. Project Scope & Core Stack

- **Objective**: Build a production-grade Lakehouse serving three downstream consumer profiles (not limited to). The actual main objective is to build and showcase my data engineering skill for switching a career to Data Engineer:
  1. **BI & F1 Enthusiasts**: Interactive dashboards for race information, tyre degradation, sector deltas, race strategy and etc.
  2. **Data Analysts**: Conformed star-schema tables for historical and race-weekend SQL analytics.
  3. **ML Engineers & Simulation Engines**: Feature Store tables for race/pit-strategy modeling and resampled telemetry frames for 2D qualifying lap replays.
- **Core Platform**: Databricks (Unity Catalog, Delta Lake, Delta Live Tables / Lakeflow, Databricks SQL, Databricks Asset Bundles).
- **Languages & Libraries**: Python, PySpark, SQL, `fastf1`, `pandas`, `numpy`, `scipy`, `requests`, `pytest`.
- **Unity Catalog Namespace**: `f1_catalog.<bronze|silver|gold>.<table_name>`


---

## 2. Data Sources & Domains

| Domain | Update Cadence | Primary Source | Key Entities / Signals |
| :--- | :--- | :--- | :--- |
| **Reference & Dimensions** | Season / Pre-race | Jolpica-F1 (Ergast) API | Drivers, constructors, circuits, seasons, race calendar. |
| **Session & Lap Timing** | Per session | FastF1 + Jolpica-F1 | Q1/Q2/Q3 classification, race results, lap times, sector splits (S1/S2/S3), speed traps. |
| **Stints & Pit Strategy** | Per session | FastF1 | Pit in/out laps, stationary duration, tyre compound, tyre freshness, stint number. |
| **Car Telemetry (`CarData`)** | ~10–12 Hz stream | FastF1 | `Speed` (km/h), `RPM`, `nGear` (1–8), `Throttle` (0–100%), `Brake` (bool), `DRS` (bitmask). |
| **Spatial Telemetry (`PosData`)** | ~3–5 Hz stream | FastF1 | `X`, `Y`, `Z` coordinates (decimeters), `Status` (`OnTrack`, `OffTrack`). |
| **Weather & Race Control** | Per minute / event | FastF1 + OpenF1 API | Air/track temp, pressure, wind, rainfall, SC/VSC flags, yellow/red flags, penalties. |


---

## 3. Medallion Architecture & Data Contracts  
All the tables listed in this section are just suggestion, can have more than these for better end result.

### 3.1 Bronze Layer (Raw, Append-Only)
- **Purpose**: Immutable landing zone preserving raw source payloads with ingestion audit metadata.
- **Ingestion Pattern**:
  - Batch/micro-batch extraction via `fastf1` and `requests`, landing raw Parquet/JSON in cloud storage, ingested into Delta via Databricks Auto Loader (`cloudFiles`) or direct Delta append.
  - Mandatory audit columns on all Bronze tables: `_ingest_timestamp` (`TIMESTAMP`), `_source_file` (`STRING`), `_batch_id` (`STRING`).
- **Critical Engineering Rule**: Never use PySpark schema inference (`spark.createDataFrame(pdf)`) on telemetry. Always pass an explicit `StructType` and convert `pd.Timedelta` columns to `float` seconds before Spark conversion.

#### Canonical Bronze Telemetry Schema (`f1_catalog.bronze.raw_telemetry`)

| Column | Spark Type | Nullable | Notes |
| :--- | :--- | :--- | :--- |
| `race_year` | `IntegerType` | `False` | Season year (e.g., `2024`) |
| `round_number` | `IntegerType` | `False` | Championship round index |
| `session_type` | `StringType` | `False` | `FP1`, `FP2`, `FP3`, `Q`, `S`, `R` |
| `driver_code` | `StringType` | `False` | 3-letter abbreviation (e.g., `VER`) |
| `driver_number` | `IntegerType` | `False` | Car number |
| `lap_number` | `IntegerType` | `False` | 1-indexed lap count |
| `stint` | `IntegerType` | `True` | Stint index |
| `compound` | `StringType` | `True` | `SOFT`, `MEDIUM`, `HARD`, `INTERMEDIATE`, `WET`, `UNKNOWN` |
| `tyre_life` | `IntegerType` | `True` | Laps completed on current set |
| `timestamp_utc` | `TimestampType` | `True` | Mapped from FastF1 `Date` |
| `session_time_seconds` | `DoubleType` | `True` | `SessionTime.dt.total_seconds()` |
| `lap_time_seconds` | `DoubleType` | `True` | `Time.dt.total_seconds()` |
| `speed_kmh` | `DoubleType` | `True` | Mapped from `Speed` |
| `engine_rpm` | `IntegerType` | `True` | Mapped from `RPM` |
| `gear` | `IntegerType` | `True` | Mapped from `nGear` |
| `throttle_pct` | `DoubleType` | `True` | Mapped from `Throttle` |
| `brake_applied` | `BooleanType` | `True` | Mapped from `Brake` |
| `drs_status` | `IntegerType` | `True` | Mapped from `DRS` |
| `distance_meters` | `DoubleType` | `True` | Integrated cumulative lap distance |
| `relative_distance` | `DoubleType` | `True` | Normalized `0.0` to `1.0` |
| `pos_x` | `DoubleType` | `True` | Spatial `X` coordinate |
| `pos_y` | `DoubleType` | `True` | Spatial `Y` coordinate |
| `pos_z` | `DoubleType` | `True` | Spatial `Z` elevation |

---

### 3.2 Silver Layer (Cleaned, Conformed, Resampled)
- **Purpose**: Schema enforcement, data quality filtering, window-based enrichments, and uniform-grid resampling.
- **Data Quality Expectations** (enforce via DLT `@dlt.expect_or_drop` or Delta `CHECK` constraints):
  - `lap_time_seconds > 0`
  - `gear BETWEEN 0 AND 8`
  - `throttle_pct BETWEEN 0 AND 100`
  - `speed_kmh >= 0`
- **Key Silver Tables**:  

  1. `f1_catalog.silver.laps_enriched`:
     - Partitioned by `(race_id, driver_id, stint_number)` ordered by `lap_number` to compute `stint_lap_index` and `rolling_3lap_avg_time`.
     - Partitioned by `(race_id, lap_number)` to compute `session_lap_fastest_time` and `delta_to_lap_leader`.
  2. `f1_catalog.silver.telemetry_resampled_10m`:
     - Buckets `distance_meters` into 10-meter bins (`(distance_meters / 10).cast("int") * 10`) so multiple drivers can be joined directly on `(race_year, round_number, lap_number, distance_bin_10m)` for corner apex and braking comparisons.


---

### 3.3 Gold Layer (Star Schema, ML Features, Simulation Output)
- **Purpose**: Query-optimized dimensional models, ML feature tables, and consumer-ready simulation datasets.
- **Clustering Strategy**: Use **Delta Lake Liquid Clustering** (`CLUSTER BY`) on high-cardinality filter keys rather than legacy Hive-style directory partitioning.
- **Dimensional & Fact Tables**:
  - Dimensions: `dim_driver`, `dim_constructor`, `dim_circuit`, `dim_race_event`.
  - Facts:
    - `f1_catalog.gold.fct_lap_performance` (`CLUSTER BY (circuit_id, race_year, driver_id)`) — joins `laps_enriched`, `dim_race`, and `weather_hourly`.
    - `f1_catalog.gold.fct_pit_stop` — stationary times, pit loss, undercut/overcut outcomes.
    - `f1_catalog.gold.fct_telemetry_resampled` — 10m-binned telemetry for analyst SQL overlays.
- **Simulation Replay Table (`f1_catalog.gold.sim_lap_frames_60fps`)**:
  - `CLUSTER BY (race_year, round_number, driver_code)`
  - Columns: `race_year`, `round_number`, `session_type`, `driver_code`, `frame_index`, `time_seconds`, `distance_meters`, `pos_x`, `pos_y`, `pos_z`, `yaw_rad`, `speed_kmh`, `engine_rpm`, `gear`, `throttle_pct`, `brake_applied`, `drs_status`, `g_lat`, `g_lon`, `delta_to_p2`.
  - **Ghost Synchronization Rule**: `delta_to_p2` is computed by indexing both Pole (P1) and P2 laps onto a shared 1-meter distance axis ($s \in [0, \text{TrackLength}]$) and taking $\Delta t(s) = t_{\text{P1}}(s) - t_{\text{P2}}(s)$.
- **ML Feature Store**:
  - Register Unity Catalog Feature Tables containing rolling driver consistency scores, compound degradation slopes, pit-window distributions, and circuit overtaking difficulty indices.

---

## 4. Agentic Implementation Roadmap

When executing this project in an AI agent environment, proceed sequentially through these phases:

### Phase 1: Repository & Databricks Asset Bundle (DAB) Setup
- Initialize repo structure:
  - `databricks.yml` (DAB configuration for `dev` and `prod` targets)
  - `src/ingestion/` (FastF1 & Jolpica extractors)
  - `src/transformations/` (PySpark Silver & Gold modules / DLT pipelines)
  - `src/simulation/` (60 FPS interpolation, physics derivation, ghost delta)
  - `tests/` (`pytest` unit tests for transformations and interpolation math)
  - `.github/workflows/` (CI/CD test & bundle deploy)
- **Acceptance Criteria**: `databricks bundle validate` passes; `pytest` runs locally with mocked SparkSession/DataFrames.

### Phase 2: Bronze Ingestion Pipeline
- Suggest to ingest from multiple sources, emphasize from API, I want to practice on handling JSON object.
- Implement `extract_session_telemetry_to_spark` with explicit `telemetry_schema` and `pd.Timedelta` conversion.
- Implement Jolpica-F1 REST extractor for historical race results and circuit metadata.
- Write to `f1_catalog.bronze.*` with Liquid Clustering enabled.
- **Acceptance Criteria**: Single race weekend (e.g., 2024 Round 1) ingests ~3–5M telemetry rows with zero schema-inference errors and valid `_ingest_timestamp` audit fields.

### Phase 3: Silver Transformations & Quality Gates
- Implement `laps_enriched` using PySpark Window functions.
- Implement `telemetry_resampled_10m` using 10-meter distance binning.
- Implement `telemetry_sim_prep` with 60 FPS cubic/nearest interpolation and yaw/$G_{\text{lat}}$/$G_{\text{lon}}$ derivations.
- Attach DLT expectations or Delta `CHECK` constraints.
- **Acceptance Criteria**: Invalid laps (`lap_time_seconds <= 0`) are quarantined/dropped; 10m-binned telemetry joins cleanly across two drivers on `(race_year, round_number, lap_number, distance_bin_10m)`.

### Phase 4: Gold Star Schema, Feature Store & Simulation Export
- Build `dim_*` and `fct_*` tables with `CLUSTER BY`.
- Build `sim_lap_frames_60fps` including `delta_to_p2` ghost synchronization.
- Register ML Feature Table in Unity Catalog.
- **Acceptance Criteria**: Gold SQL queries execute in sub-second latency; `sim_lap_frames_60fps` exports cleanly to JSON for 2D/3D replay rendering.

### Phase 5: Downstream Showcases & Benchmarks
- Create Databricks SQL Dashboard (tyre degradation slopes, sector speed deltas).
- Create ML notebook (XGBoost pit-window prediction using Feature Store).
- Document query latency benchmarks comparing unclustered vs. Liquid Clustered Gold tables in `README.md`.

---

## 5. Agent Guardrails & Coding Standards

1. **No Schema Inference on Telemetry**: Never call `spark.createDataFrame(pdf)` without `schema=telemetry_schema`.
2. **Timedelta Conversion**: Always convert `pd.Timedelta` columns (`SessionTime`, `Time`) via `.dt.total_seconds()` before Spark ingestion.
3. **Liquid Clustering Over Partitioning**: Use `CLUSTER BY (...)` on telemetry and lap fact tables instead of `PARTITIONED BY` to avoid small-file skew.
4. **Distance-Based Joins for Driver Comparisons**: Never join two drivers' telemetry on `timestamp_utc`; always join on `distance_bin_10m` or distance-indexed splines.
5. **Testability**: Keep pure PySpark DataFrame transformation functions decoupled from `spark.read` / `spark.write` I/O so `pytest` can validate logic with synthetic DataFrames in CI.
6. **Act as a Consultant or Senior Engineer**: Provide suggestion without generating script if I don't ask to do. I want to think and train myself.
7. **Test, Staging, Prod**: I am using a databricks free edition (not the comunity edition), try to lower the amount of input and data to be process during development to avoid exceeding the compute limit.
