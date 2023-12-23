# Project Requirements

This paragraph outlines the Python libraries and their versions required for this project.

## Libraries and Versions

- **Python:** 3.11.2
- **Pymongo:** 4.6.0
- **Psycopg2:** 2.9.9
- **Telegram Version:** 20.7
- **Requests Version:** 2.31.0

### Install Libraries

```bash
pip install -r requirements.txt
```

## Usage

1. **Data Crawling**:
    - Open the `data_crawling.ipynb` notebook.
    - Run each cell in the notebook in sequential order.

2. **CSV Conversion**:
    - Open the `csv_conversion.ipynb` notebook.
    - Run each cell in the notebook in sequential order.

3. **Upload files to Google Bucket**:
    - Access [Google Bucket](https://console.cloud.google.com/storage/browser/it4043e-it5384/it4043e/it4043e_group2_problem5/data/csv?pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))&authuser=0&cloudshell=false&orgonly=true&supportedpurview=organizationId&prefix=&forceOnObjectsSortingFiltering=false) from this link.

3. **Access JupyterLab**:
    - Access [JupyterLab](http://35.197.133.53:1201/user/group02/lab).
    - Use the provided credentials:
        - **Username:** group02
        - **Password:** e10bigdata

4. **Run `bot.py`**:
    - Open Terminal 1 in JupyterLab.
    - Run the command `python bot.py`.

5. **Run `process.py`**:
    - Open Terminal 2 in JupyterLab.
    - Run the command `conda activate process` to activate the environment.
    - Run the command `python process.py`.

## Data definition

The data crawled for project comes from the centic.io database.

We crawl a predefined amount of ethereum wallets with a minimum balanceInUSD of 100_000$. 

For each wallet we crawl related incoming and outgoing transaction.

For each transaction we crawl related transferring event and smart contract if existent.