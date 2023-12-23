from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col, when, avg, sum as spark_sum, count as spark_count, expr, desc
from decimal import Decimal
import requests

##Configuration

#Telegram vars
TOKEN = "6649302734:AAHRn_UtVt3VbvXIZy8BTU1OJT9wEbE6ZR4"
chat_id = "6868668232"

#config the connector jar file
spark = (SparkSession.builder.appName("SimpleSparkJob").master("spark://34.142.194.212:7077")
         .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
         .config("spark.executor.memory", "1G")  
         .config("spark.driver.memory","1G") 
         .config("spark.executor.cores","1") 
         .config("spark.python.worker.memory","1G")
         .config("spark.driver.maxResultSize","1G") 
         .config("spark.kryoserializer.buffer.max","1024M")
         .config("spark.port.maxRetries", "100")
         .getOrCreate())
#config the credential to identify the google cloud hadoop file 
spark.conf.set("google.cloud.auth.service.account.json.keyfile","/opt/bucket_connector/lucky-wall-393304-3fbad5f3943c.json")
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

# Define file paths
smart_contracts_path = "gs://it4043e-it5384/it4043e/it4043e_group2_problem5/data/csv/smart_contracts.csv"
ethereum_wallets_path = "gs://it4043e-it5384/it4043e/it4043e_group2_problem5/data/csv/ethereum_wallets.csv"
incoming_transactions_path = "gs://it4043e-it5384/it4043e/it4043e_group2_problem5/data/csv/incoming_transactions.csv"
outgoing_transactions_path = "gs://it4043e-it5384/it4043e/it4043e_group2_problem5/data/csv/outgoing_transactions.csv"
native_token_price_change_logs_path = "gs://it4043e-it5384/it4043e/it4043e_group2_problem5/data/csv/native_token_price_change_logs.csv"
transferring_events_path = "gs://it4043e-it5384/it4043e/it4043e_group2_problem5/data/csv/transferring_events_formatted.csv.csv"

# Read CSV files into DataFrames
df_smart_contracts = spark.read.csv(smart_contracts_path, header=True, inferSchema=True)
df_smart_contracts = df_smart_contracts.filter(df_smart_contracts["price_change_logs"] != "NULL")
df_ethereum_wallets = spark.read.csv(ethereum_wallets_path, header=True, inferSchema=True)
df_incoming_transactions = spark.read.csv(incoming_transactions_path, header=True, inferSchema=True)
df_outgoing_transactions = spark.read.csv(outgoing_transactions_path, header=True, inferSchema=True)
df_native_token_price_change_logs = spark.read.csv(native_token_price_change_logs_path, header=True, inferSchema=True)
df_transferring_events = spark.read.csv(transferring_events_path, header=True, inferSchema=True)

native_token_price_change_logs_map = {}
df_native_token_price_change_logs_rows = df_native_token_price_change_logs.collect()
for row in df_native_token_price_change_logs_rows: 
    native_token_price_change_logs_map[row["timestamp"]] = row["value_in_usd"]
    

# Register DataFrames as SQL temporary views
df_smart_contracts.createOrReplaceTempView("smart_contracts")
df_ethereum_wallets.createOrReplaceTempView("ethereum_wallets")
df_incoming_transactions.createOrReplaceTempView("incoming_transactions")
df_outgoing_transactions.createOrReplaceTempView("outgoing_transactions")
df_native_token_price_change_logs.createOrReplaceTempView("native_token_price_change_logs")
df_transferring_events.createOrReplaceTempView("transferring_events")

#Querying outgoing transactions joined with transferring events and smart contracts to retrieve
# processable data on transactions (such as value in token and value in wei)
query = """
    SELECT 
        *
    FROM
        outgoing_transactions ot JOIN transferring_events te ON ot.transaction_hash = te.transaction_hash
                                 JOIN smart_contracts sc ON te.contract_address = sc.contract_address
 
"""


# Execute the SQL query
result_df = spark.sql(query)

def find_closest_lowest_timestamp_value(target_timestamp):
    result = df_native_token_price_change_logs.filter(df_native_token_price_change_logs['timestamp'] <= target_timestamp) \
               .sort('timestamp', ascending=False) \
               .select('value_in_usd').first()
    return result.value_in_usd if result is not None else None
    

@udf(StringType())
def get_price_of_eth_in_usd(price_change_logs_row_entry: str, timestamp: str):
    timestamp = int(timestamp)

    price_change_logs_row_entry = price_change_logs_row_entry.replace("{", "").replace("}", "").replace(" ", "")
    price_change_logs = price_change_logs_row_entry.split(",")
    price_change_logs_dict = {}

    for item in price_change_logs:
        key_value_pair = item.split(":")
        try:
            key = int(key_value_pair[0])
            value = Decimal(key_value_pair[1])  # Use Decimal for precision
            price_change_logs_dict[key] = value
        except ValueError:
            pass  

    # Find the largest timestamp less than or equal to the target timestamp
    try:
        next_lowest_timestamp = max(key for key in price_change_logs_dict.keys() if key <= timestamp)
        price_of_eth_in_usd = float(price_change_logs_dict[next_lowest_timestamp])
    except ValueError:  # Handle empty dictionary or no suitable timestamps
        price_of_eth_in_usd = "NULL"

    return str(price_of_eth_in_usd)

result_df = result_df.withColumn("price_of_token_in_usd", get_price_of_eth_in_usd(result_df["price_change_logs"], result_df["timestamp"]))

#Ignoring null values
result_df = result_df.filter(result_df["price_of_token_in_usd"] != "NULL")
result_df = result_df.withColumn("price_of_token_in_usd", col("price_of_token_in_usd").cast("float"))

#Calculate transaction value in USD based on wei value and token value
result_df = result_df.withColumn(
    "transaction_value_in_usd",
    when(result_df["value_in_wei"] == 0, result_df["value_in_token"] * result_df["price_of_token_in_usd"])
    .otherwise((result_df["value_in_wei"] * find_closest_lowest_timestamp_value(col("timestamp"))) / 10**18)
)

# Filter out rows where 'transaction_value_in_usd' is not NULL
result_df = result_df.filter(result_df["transaction_value_in_usd"].isNotNull())

# Show the final DataFrame with relevant columns
result_df.select("ot.transaction_hash", "timestamp", "ot.value_in_wei", "price_of_token_in_usd", "transaction_value_in_usd").show()

def calculate_transaction_summary(df):
    
    # Calculate the sum of transaction_value_in_usd
    total_transaction_value = df.select(spark_sum("transaction_value_in_usd")).collect()[0][0]

    # Calculate the count of total transactions
    total_transaction_count = df.select(spark_count("*")).collect()[0][0]
    
    #Calculate the avergae of total transaction
    total_transaction_average = df.select(avg('transaction_value_in_usd')).first()[0]
    
    # Calculate the median
    median_expr = expr('percentile_approx(transaction_value_in_usd, 0.5)')

    # Execute the expression and collect the result
    median_result = df.agg(median_expr).collect()[0][0]

    return total_transaction_value, total_transaction_count, total_transaction_average, median_result

#Print useful values of transactions
total_value, total_count, average, median = calculate_transaction_summary(result_df)

print("Total Transaction Value: $", total_value)
print("Total Transaction Count: ", total_count)
print("Transaction Average: ", average)
print("Transaction Median: ", median)

# Order the DataFrame by 'transaction_value_in_usd' in descending order to show the highest transactions
sorted_df = result_df.orderBy(desc('transaction_value_in_usd'))

##Retrieve latest high risk transaction
def get_last_high_risk_transactions():
    high_risk_transactions = result_df.filter(result_df["transaction_value_in_usd"] >= average * 10)
    last_high_risk_transaction_timestamp = high_risk_transactions.orderBy(desc('timestamp')).select('timestamp').first()[0]
    return last_high_risk_transaction_timestamp

#Showcase top transactions with highest value
top_10_transactions = sorted_df.limit(10)
top_10_transactions.select("ot.transaction_hash", "timestamp", "ot.value_in_wei", "price_of_token_in_usd", "transaction_value_in_usd").show()

#Send message to bot
last_high_risk_transaction_timestamp = get_last_high_risk_transactions()
message = f"Last high risk transaction detected at timestamp: {last_high_risk_transaction_timestamp} "
url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
print(requests.get(url).json()) # this sends the message

spark.stop # Ending spark job
