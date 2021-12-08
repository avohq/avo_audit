# Unreleased
Features and bug fixes that have been applied but not released yet.

# avo-audit v1.0.1

## Fixes and polish
- mininum_avg_event_volume parameter
  Allows the user to set the mininum avarage for the algorithm to try to detect anomalies, as it can be extremely hard to detect with very low volume events.

# avo-audit v1.0.0

## Fixes and Polish

- `Schema_to_table` works against snowflake
- `Schema_to_table` only queries tables instead of all relations in a schema

## Features

- Event anomaly detection.  use `detect_event_anomaly` macro to see if any spikes or drop in raw events are occuring over a time period.
Can be set up as a test that can be run every day for automization.
