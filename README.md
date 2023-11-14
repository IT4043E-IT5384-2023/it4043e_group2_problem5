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

We crawl a predefined number of wallets with their corresponding incoming and outgoing transactions as well as the transferring events and smart contracts related to them if they exist. 

Next to this data we also crawl the native token priceChangeLog to define the value of transactions that not have a smart contract. 
