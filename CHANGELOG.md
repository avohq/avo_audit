# Unreleased
Features and bug fixes that have been applied but not released yet.


# avo-audit v1.0.1
- mininum_avg_event_volume parameter
  Allows the user to set the mininum avarage for the algorithm to try to detect anomalies, as it can be extremely hard to detect with very low volume events.

# avo-audit v1.0.0

## Features

- Event anomaly detection.  use `detect_event_anomaly` macro to see if any spikes or drop in raw events are occuring over a time period.
Can be set up as a test that can be run every day for automization.
