import argparse
import json
import os
import pandas as pd
import sqlite3 as sq
import time

from collections import defaultdict
from time_util import calc_slot_timestamp


SCP = [
    '0xa69babef1ca67a37ffaf7a485dfff3382056e78c',
    '0x56178a0d5f301baf6cf3e1cd53d9863437345bf9',
    '0xa57bd00134b2850b2a1c55860c9e9ea100fdd6cf',
    '0x4cb18386e5d1f34dc6eea834bf3534a970a3f8e7',
    '0x5050e08626c499411b5d0e0b5af0e83d3fd82edf',
    '0xfa103c21ea2df71dfb92b0652f8b1d795e51cdef',
    '0x0da9d9ecea7235c999764e34f08499ca424c0177'
]
SCP = [i.lower() for i in SCP]


WINTERMUTE = [
    '0x0000006daea1723962647b7e189d311d757fb793',
    '0x00000000ae347930bd1e7b0f35588b92280f9e75',
    '0x0087bb802d9c0e343f00510000729031ce00bf27',
    '0xaf0b0000f0210d0f421f0009c72406703b50506b',
    '0x280027dd00ee0050d3f9d168efd6b40090009246',
    '0x51c72848c68a965f66fa7a88855f9f7784502a7f',
    '0xec6fc9be2d5e505b40a2df8b0622cd25333823db'
]
WINTERMUTE = [i.lower() for i in WINTERMUTE]


JUMP = [
    '0x9507c04b10486547584c37bcbd931b2a4fee9a41',
    '0xf584f8728b874a6a5c7a8d4d387c9aae9172d621',
    '0x2d35ce0cabf4ac263eab1c182a454c91cc155cc1'
]
JUMP = [i.lower() for i in JUMP]


coffee = [
    '0xC0ffeEBABE5D496B2DDE509f9fa189C25cF29671'.lower(),
    '0x3aa228a80f50763045bdfc45012da124bd0a6809'.lower(),
    '0xe08d97e151473a848c3d9ca3f323cb720472d015'.lower(),
]


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

        if txn["Sigma"]:
            sources.append("Sigma")

        if txn["from"].lower() == "0xae2fc483527b8ef99eb5d9b44875f005ba1fae13" or (txn["to"] is not None and txn["to"].lower() == "0x93ffb15d1fa91e0c320d058f00ee97f9e3c50096"):
            sources.append("jaredfromsubway.eth")
        elif txn["to"] is not None and txn["to"].lower() in SCP:
            sources.append("SCP")
        elif txn["to"] is not None and txn["to"].lower() in WINTERMUTE:
            sources.append("WINTERMUTE")
        elif txn["to"] is not None and txn["to"].lower() in JUMP:
            sources.append("JUMP")
        elif txn["to"] is not None and txn["to"].lower() in coffee:
            sources.append("c0ffeebabe.eth")
        elif txn["from"].lower() in searchers:
            sources.append("Searcher: " + txn["from"].lower())
        elif type(txn["to"]) == str and txn["to"].lower() in searchers:
            sources.append("Searcher: " + txn["to"].lower())
        elif txn["to"] is not None:
            sources.append("Contract: " + txn["to"].lower())
        else:
            sources.append("User: " + txn["from"].lower())

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
    slot_to_public_value = dict(zip(blocks_df["slot"], blocks_df["public"]))

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
        public_value = slot_to_public_value[slot] * 1e18

        if slot in winning_bid_timestamp:
            timestamp = max(winning_bid_timestamp[slot], slot_to_timestamp[slot])
            slot_df = slot_df[slot_df["timestamp_ms"] <= timestamp]
        
        other_bids = slot_df[(slot_df["value"] < winning_bid_value) & (slot_df["builder"] != winner)]
        next_highest_bid_value = other_bids["value"].max()

        if pd.isna(next_highest_bid_value):
            continue
        
        for provider, profit in provider_profits[number].items():
            if winning_block_value - profit < next_highest_bid_value:
                pivotal_providers.append((date_str, number, slot, winning_bid_value, winning_block_value, float(next_highest_bid_value), public_value, winner, provider, profit))

    print(f"Identify pivotal providers {time.time()-t:.2f}s")

    return pd.DataFrame(pivotal_providers, columns=["date", "number", "slot", "bid_value", "block_value", "next_highest_bid_value", "public_value", "winner", "provider", "txn_fee"])


def identify_pivotal_builders(private_transactions_path, blocks_path, bids_folder_path, db_path, builders):
    print("Loading data...")
    private_transactions = os.listdir(private_transactions_path)
    private_transactions = [i for i in private_transactions if i.endswith(".parquet")]

    blocks_df = pd.read_parquet(blocks_path)
    blocks_df = blocks_df[blocks_df["builder_pubkey"].notnull()].reindex()
    blocks_df["builder"] = blocks_df["builder_pubkey"].apply(lambda x: builders.get(x, x[:12]))
    blocks_df["timestamp"] = blocks_df["slot"].apply(calc_slot_timestamp)
    blocks_df["date"] = blocks_df["timestamp"].dt.strftime("%Y%m%d")

    print("Start identifying pivotal builders by date")
    current_month = None
    private_transactions_df = pd.DataFrame()
    for date_str, date_df in blocks_df.groupby("date"):
        # load private transactions for current month
        month_str = date_str[:6]
        if month_str != current_month:
            current_month = month_str
            for month_file in private_transactions:
                if current_month in month_file:
                    private_transactions_df = pd.read_parquet(os.path.join(private_transactions_path, month_file))
                    private_transactions_df["source"] = private_transactions_df.apply(get_source_from_txn, axis=1)
                    print(f"Loaded {month_file} for {current_month}")
                    break
                    
        block_numbers = date_df["number"].unique()
        daily_private_transactions_df = private_transactions_df[private_transactions_df["blockNumber"].isin(block_numbers)].reindex()

        pivotal_providers_df = parse_date(bids_folder_path, date_str, date_df, daily_private_transactions_df)
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
    cur.execute("CREATE TABLE pivotal_providers (date TEXT, number INTEGER, slot INTEGER, bid_value FLOAT, block_value FLOAT, next_highest_bid_value FLOAT, public_value FLOAT, winner TEXT, provider TEXT, txn_fee FLOAT)")
    con.commit()
    con.close()

    data_folder_path = args.data_folder_path

    searchers_df = pd.read_csv(os.path.join(data_folder_path, "searchers.csv"))
    searchers = set(searchers_df["address"].to_list())

    with open(os.path.join(data_folder_path, "builders.json"), "r") as f:
        builders = json.load(f)

    builders = {i:k for k, v in builders.items() for i in v}

    identify_pivotal_builders(args.private_transactions_path, args.blocks_path, args.bids_folder_path, db_path, builders)

