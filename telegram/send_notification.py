import requests

TOKEN = "6649302734:AAHRn_UtVt3VbvXIZy8BTU1OJT9wEbE6ZR4"
chat_id = "6868668232"
message = "hello from your telegram bot"
url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
print(requests.get(url).json()) # this sends the message

result_df = result_df.withColumn( "transaction_value_in_usd", when(result_df["value_in_wei"] == 0, result_df["value_in_token"] * result_df["price_of_token_in_usd"]).otherwise((result_df["value_in_wei"] * find_closest_lowest_timestamp_value(col("timestamp"))) / 10**18))