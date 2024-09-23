import pandas as pd

from functools import lru_cache


THE_MERGE_BLOCK_NUMBER = 15537394
THE_MERGE_SLOT = 4700013
THE_MERGE_BLOCK_TIMESTAMP = pd.to_datetime("2022-09-15 06:42:59")
SLOT_TIME = 12


@lru_cache(maxsize=1000)
def calc_slot_timestamp(slot):
    slot_gap = slot - THE_MERGE_SLOT
    time_gap = pd.Timedelta(slot_gap * SLOT_TIME, unit="s")
    return THE_MERGE_BLOCK_TIMESTAMP + time_gap


@lru_cache(maxsize=1000)
def get_timestamp_slot(timestamp):
    time_gap = timestamp - THE_MERGE_BLOCK_TIMESTAMP
    slot_gap = time_gap.total_seconds() / SLOT_TIME
    slot = THE_MERGE_SLOT + int(slot_gap)
    return slot
