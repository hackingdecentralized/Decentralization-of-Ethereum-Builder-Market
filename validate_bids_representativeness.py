import argparse
import json
import matplotlib.pyplot as plt
import os
import pandas as pd
import seaborn as sns


# Dataset Time Range
dates = [
    pd.date_range("2023-04-09", "2023-04-15"),
    pd.date_range("2023-05-01", "2023-05-07"),
    pd.date_range("2023-06-01", "2023-06-07"),
    pd.date_range("2023-07-01", "2023-07-07"),
    pd.date_range("2023-08-01", "2023-08-07"),
    pd.date_range("2023-09-01", "2023-09-07"),
    pd.date_range("2023-10-01", "2023-10-07"),
    pd.date_range("2023-11-01", "2023-11-07"),
    pd.date_range("2023-12-01", "2023-12-07"),
]

# Load Builders
def load_builder(data_path):
    with open(os.path.join(data_path, "builders.json"), "r") as f:
        builders = json.load(f)

    pubkey_to_builder = {}
    for builder, data in builders.items():
        for item in data:
            pubkey_to_builder[item] = builder

    return pubkey_to_builder

# Load Blocks
def load_blocks(data_path, pubkey_to_builder):
    block_df_path = os.path.join(data_path, "eth_blocks.parquet")
    block_df = pd.read_parquet(block_df_path)
    block_df["builder"] = block_df["builder_pubkey"].apply(lambda x: pubkey_to_builder.get(x, x[:10]) if x is not None else None)
    block_slot_builder = dict(zip(block_df["slot"], block_df["builder"]))

    return block_slot_builder

# Load Bids
def load_bids(bids_path, dates, pubkey_to_builder):
    data = []
    for sub_date in dates:
        for date in sub_date:
            date_str = date.strftime("%Y%m%d")
            print(date_str)
            df = pd.read_parquet(os.path.join(bids_path, f"{date_str}.parquet"))
            df["builder"] = df["builder_pubkey"].apply(lambda x: pubkey_to_builder.get(x, x[:10]))
            for slot, slot_df in df.groupby("slot"):
                data.append((date_str, slot, list(slot_df["builder"].unique())))

    bid_slot_builder = {}
    for date_str, slot, builders in data:
        bid_slot_builder[slot] = set(builders)

    return bid_slot_builder

# Compute Representativeness
def compute_representativeness(data_path, block_builders, bid_builders):
    dir_path = os.path.join(data_path, "index")
    files = os.listdir(dir_path)
    new_data = []
    for f in files:
        if f.startswith("capability_"):
            capability_csv = pd.read_csv(f"{dir_path}/{f}")
            for slot, slot_df in capability_csv.groupby("slot"):
                builders = set(slot_df["builder"].values)
                total_builders = set(builders)
                if slot in bid_builders:
                    total_builders |= bid_builders[slot]
                if block_builders.get(slot) is not None:
                    total_builders.add(block_builders[slot])

                top5_builders = set(['beaverbuild', 'rsync-builder', 'builder0x69', 'Flashbots', 'Titan'])

                new_data.append((slot, "All Builder", 100*len(builders)/ len(total_builders)))
                new_data.append((slot, "Top-5 Builders", 100*len(top5_builders&builders)/len(top5_builders&total_builders)))
    return pd.DataFrame(new_data, columns=["slot", "builders", "percentage"])

# Plot Violin Plot
def plot(data_df, output_path):
    plt.cla()
    sns.set_style("whitegrid")
    plt.figure(figsize=(15, 4), dpi=300)
    sns.violinplot(hue="builders", x="builders", y="percentage", data=data_df, density_norm='width', inner="quartile", cut=0, legend=False)
    plt.xlabel(None)
    plt.ylabel("percentage", fontsize=32)
    plt.xticks(fontsize=32)
    plt.yticks(fontsize=32)
    plt.ylim(0, 100)
    plt.savefig(os.path.join(output_path, "bid_validation.pdf"), bbox_inches='tight')

# A script to validate the representativeness of bids from ultra sound relay.
# You need to download partial bids dataset to enable the script to run.

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, default="./data")
    parser.add_argument("--bids_folder_path", type=str, default="./partial")
    parser.add_argument("--output_path", type=str, default="./images")
    args = parser.parse_args()
    data_path = args.data_path
    bids_folder_path = args.bids_folder_path
    builders = load_builder(data_path)
    block_builders = load_blocks(data_path, builders)
    bid_builders = load_bids(bids_folder_path, dates, builders)
    data_df = compute_representativeness(data_path, block_builders, bid_builders)
    plot(data_df, args.output_path)


if __name__ == "__main__":
    main()
