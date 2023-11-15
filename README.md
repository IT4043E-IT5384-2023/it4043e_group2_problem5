# Project Requirements

This paragraph outlines the Python libraries and their versions required for this project.

## Libraries and Versions

- **Python:** 3.11.2
- **Pymongo:** 4.6.0
- **Psycopg2:** 2.9.9

### Install Libraries

```bash
pip install -r requirements.txt
```

## Usage

Run each cell in **main.ipynb** in the corresponding order to 

## Data definition

The data crawled for project comes from the centic.io database.

We crawl a predefined amount of ethereum wallets with a minimum balanceInUSD of 100_000$. 

For each wallet we crawl related incoming and outgoing transaction.

For each transaction we crawl related transferring event and smart contract if existent.