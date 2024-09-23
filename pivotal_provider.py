import argparse
import json
import os
import pandas as pd
import sqlite3 as sq
import time

from collections import defaultdict
from time_util import calc_slot_timestamp


def get_source_from_txn(txn):
    global searchers

    sources = []

    if txn["MEV-Share"]:
        sources.append("MEV-Share")
    
    if txn["MEV Blocker"]:
        sources.append("MEV Blocker")

    if not txn["MEV-Share"] and not txn["MEV Blocker"]:
        if txn["Maestro"]:
            sources.append("Maestro")

        if txn["Banana Gun"]:
            sources.append("Banana Gun")
        
        if txn["Unibot"]:
            sources.append("Unibot")

        if txn["from"].lower() == "0xae2fc483527b8ef99eb5d9b44875f005ba1fae13":
            sources.append("jaredfromsubway.eth")
        elif txn["from"].lower() in searchers:
            sources.append("Searcher: " + txn["from"].lower())
        elif type(txn["to"]) == str and txn["to"].lower() in searchers:
            sources.append("Searcher: " + txn["to"].lower())

    return tuple(sources)


def parse_date(bids_folder_path, date_str, blocks_df, private_transactions_df):
    t = time.time()
    slots = blocks_df["slot"].unique()
    block_hashes = blocks_df["block_hash"].unique()
    slot_to_number = dict(zip(blocks_df["slot"], blocks_df["number"]))
    slot_to_winner = dict(zip(blocks_df["slot"], blocks_df["builder"]))
    slot_to_block_value = dict(zip(blocks_df["slot"], blocks_df["block_value"]))
    slot_to_bid_value = dict(zip(blocks_df["slot"], blocks_df["bid_value"]))
    slot_to_timestamp = dict(zip(blocks_df["slot"], blocks_df["timestamp"]))

    bids_df = pd.read_parquet(f"{bids_folder_path}/{date_str}.parquet", engine="fastparquet")
    bids_df["timestamp_ms"] = pd.to_datetime(bids_df["timestamp_ms"], format="mixed")
    bids_df["value"] = pd.to_numeric(bids_df["value"], errors="coerce")
    bids_df["builder"] = bids_df["builder_pubkey"].apply(lambda x: builders.get(x, x[:12]))
    print(f"Read {date_str} bids_df {time.time()-t:.2f}s")

    t = time.time()
    winning_bid_timestamp = {}

    sub_bids_df = bids_df[bids_df["block_hash"].isin(block_hashes)].reindex()
    for _, row in sub_bids_df.iterrows():
        slot = row["slot"]
        timestamp = row["timestamp_ms"]
        if slot not in winning_bid_timestamp:
            winning_bid_timestamp[slot] = timestamp
        winning_bid_timestamp[slot] = min(winning_bid_timestamp[slot], timestamp)

    print(f"Compute winning bid timestamp {time.time()-t:.2f}s")

    t = time.time()
    # calculate MEV from providers' transactions
    provider_profits = defaultdict(lambda: defaultdict(int))
    private_profits = defaultdict(int)
    for block_number, block_df in private_transactions_df.groupby("blockNumber"):
        for txn_fee, source in block_df[["txn_fee", "source"]].values:
            for s in source:
                provider_profits[block_number][s] += txn_fee
            private_profits[block_number] += txn_fee
    print(f"Compute provider MEV {time.time()-t:.2f}s")

    # identify pivotal builders
    t = time.time()
    pivotal_providers = []

    for slot, slot_df in bids_df.groupby("slot"):
        if slot not in slots:
            continue

        number = slot_to_number[slot]
        winner = slot_to_winner[slot]
        winning_bid_value = slot_to_bid_value[slot] * 1e18
        winning_block_value = slot_to_block_value[slot] * 1e18

        if slot in winning_bid_timestamp:
            timestamp = max(winning_bid_timestamp[slot], slot_to_timestamp[slot])
            slot_df = slot_df[slot_df["timestamp_ms"] <= timestamp]
        
        other_bids = slot_df[(slot_df["value"] < winning_bid_value) & (slot_df["builder"] != winner)]
        next_highest_bid_value = other_bids["value"].max()

        if pd.isna(next_highest_bid_value):
            continue
        
        for provider, profit in provider_profits[number].items():
            if winning_block_value - profit < next_highest_bid_value:
                pivotal_providers.append((date_str, number, slot, winning_block_value, winner, provider, profit))

    print(f"Identify pivotal providers {time.time()-t:.2f}s")

    return pd.DataFrame(pivotal_providers, columns=["date", "number", "slot", "value", "winner", "provider", "profit"])


def identify_pivotal_builders(private_transactions_path, blocks_path, bids_folder_path, db_path, builders):
    print("Loading data...")
    private_transactions_df = pd.read_parquet(private_transactions_path)
    private_transactions_df["txn_fee"] = pd.to_numeric(private_transactions_df["txn_fee"], errors="coerce")

    blocks_df = pd.read_parquet(blocks_path)
    blocks_df = blocks_df[blocks_df["builder_pubkey"].notnull()].reindex()
    blocks_df["builder"] = blocks_df["builder_pubkey"].apply(lambda x: builders.get(x, x[:12]))
    blocks_df["timestamp"] = blocks_df["slot"].apply(calc_slot_timestamp)
    blocks_df["date"] = blocks_df["timestamp"].dt.strftime("%Y%m%d")

    print("Start identifying pivotal builders by date")
    for date_str, date_df in blocks_df.groupby("date"):
        block_numbers = date_df["number"].unique()
        daily_private_transactions_df = private_transactions_df[private_transactions_df["blockNumber"].isin(block_numbers)].reindex()
        daily_private_transactions_df["source"] = daily_private_transactions_df.apply(get_source_from_txn, axis=1)

        pivotal_providers_df = parse_date(bids_folder_path, date_str, blocks_df, daily_private_transactions_df)
        con = sq.connect(db_path)
        pivotal_providers_df.to_sql("pivotal_providers", con, if_exists="append", index=False)
        con.commit()
        con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_path", type=str, required=True)
    parser.add_argument("--private_transactions_path", type=str, required=True)
    parser.add_argument("--blocks_path", type=str, required=True)
    parser.add_argument("--bids_folder_path", type=str, required=True)
    parser.add_argument("--data_folder_path", type=str, required=True)

    args = parser.parse_args()
    db_path = args.db_path

    con = sq.connect(db_path)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS pivotal_providers")
    cur.execute("CREATE TABLE pivotal_providers (date TEXT, number INTEGER, slot INTEGER, value FLOAT,  winner TEXT, provider TEXT, profit FLOAT)")
    con.commit()
    con.close()

    data_folder_path = args.data_folder_path

    searchers_df = pd.read_csv(os.path.join(data_folder_path, "searchers.csv"))
    searchers = set(searchers_df["address"].to_list())

    with open(os.path.join(data_folder_path, "builders.json"), "r") as f:
        builders = json.load(f)

    builders = {i:k for k, v in builders.items() for i in v}

    identify_pivotal_builders(args.private_transactions_path, args.blocks_path, args.bids_folder_path, db_path, builders)
