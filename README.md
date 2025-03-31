# Decentralization of Ethereum's Builder Market

This repository holds the data and code used for the paper "Decentralization of Ethereum's Builder Market".


## Data

Note: Please download `eth_blocks.parquet` and `level.db` from our [public datasets](https://doi.org/10.7910/DVN/3X7X2U) and place them in the `data` folder. Also, download all `private_transactions_{month}.parquet` files and place them in the `data/private_transactions` folder to ensure the code works properly.

Below is a brief introduction to the datasets provided in this paper and their potential benefits for future research.

### Ethereum blocks

[eth_blocks.parquet](https://dataverse.harvard.edu/api/access/datafile/11037520) is a Parquet file that stores all blocks and winning bids from historical MEV-Boost auctions between September 2022 and October 2024. It includes the bid value, true value, builder, and relay information for each block produced through MEV-Boost, making it useful for future analyses of MEV-Boost auctions.

### Private order flows

The `data/private_transactions` folder consists of monthly Parquet files named as `private_transactions_{month}.parquet`, each storing private order flows, their sources, and values from September 2022 to October 2024.

### Builder information

JSON files containing mappings between builder public keys (addresses) and builder identities are available in the `data` folder. Our labeled identities help researchers and developers quickly identify builders, saving time on MEV-Boost auction data analysis.

- `builders.json`: The mapping between builder public keys and builder identities.
- `addresses.json`: The mapping between builder addresses and builder identities.

### Searcher information

The CSV file contains the addresses of the searchers' smart contracts.

### Partial bids

The partial bids for historical MEV-Boost auctions between September 2022 and October 2024 are available in the [public datasets](https://doi.org/10.7910/DVN/3X7X2U).

This dataset includes the public keys and bids of all builders in historical MEV-Boost auctions, offering a granular view of auction dynamics and enabling researchers to model the bidding behavior of builders.

### Computed true values

The `data/index` folder includes all intermediate results of true values we computed from historical MEV-Boost auctions (April 9-15 and May 1-7 to December 1-7, 2023).

The computed true values for auctions provide insights into the actual value of bids in historical MEV-Boost auctions, allowing for the evaluation of the competition among builders and the inequality in their block-building capacities.

### Pivotal providers

[level.db](https://dataverse.harvard.edu/api/access/datafile/11037519) is a SQLite database file that stores *pivotal providers* from historical MEV-Boost auctions between September 2022 and October 2024. This file can help researchers identify key sources of valuable transactions in these auctions and assess their impact on outcomes.


## Reproducing Results

[Python3](https://www.python.org/downloads/) and [Jupyter Notebook](https://jupyter-notebook-beginner-guide.readthedocs.io/en/latest/install.html) are required to reproduce the figures and tables, and you can run `pip install -r requirements.txt` to install the following libraries:

```
fastparquet==2024.5.0
matplotlib==3.9.1.post1
numpy==1.26.4
pandas==2.2.2
seaborn==0.13.2
scipy==1.14.1
```

### Structure of the repository

- `data`: The folder for data.
- `images`: The folder where the output images are stored.
- `pivotal_provider.py`: A script to compute pivotal providers from historical MEV-Boost auctions between September 2022 and October 2024.
- `plot.ipynb`: A Jupyter notebook to reproduce all results in the paper.
- `time_util.py`: A script that defines time utility functions for computing Ethereum slots.
- `validate_bids_representativeness.py`: A script to validate the representativeness of the bids from ultra sound relay.

### Simulation framework

The simulation framework we built for computing the true value of historical bids is available in our [reth fork](https://github.com/hackingdecentralized/reth).


## Acknowledging Our Work

Consider cite our work if you find our contributions helpful!
```bibtex
@inproceedings{yang2025decentralization,
  title={Decentralization of Ethereum's Builder Market},
  author={Yang, Sen and Nayak, Kartik and Zhang, Fan},
  booktitle = {2025 IEEE Symposium on Security and Privacy (SP)},
  year = {2025},
  organization = {IEEE}
}
```