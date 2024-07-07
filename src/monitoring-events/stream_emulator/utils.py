import json

# Factor to scale the emulation time.
# 1 hour of real-time is emulated in 1 second.
FAST_SPEED_FACTOR = 36000
NORMAL_SPEED_FACTOR = 3600


# Interval for flushing the queue to ensure it doesn't get overloaded.
# This helps manage bursts of events that have the same timestamp.
FAST_FLUSHING_INTERVAL = 0.5  # seconds
NORMAL_FLUSHING_INTERVAL = 5  # seconds


def tuple_for_last_window_triggering(event: tuple) -> str:
    l_event = list(event)
    # Set fileds values so that the tuple passes flink filtering, allowing the last window to be triggered. This tuple won't impact query results, since it will not be part of the windows of interest.
    l_event[0] = "2023-04-30T00:00:00.000000"
    l_event[3] = "1"
    l_event[4] = "1000"

    return json.dumps(tuple(l_event))


def scale_factor(is_fast: bool) -> int:
    return FAST_SPEED_FACTOR if is_fast else NORMAL_SPEED_FACTOR


def flush_interval(is_fast: bool) -> float:
    return FAST_FLUSHING_INTERVAL if is_fast else NORMAL_FLUSHING_INTERVAL
