# Importing necessary libraries
import time
import json
import requests 
import os
from dotenv import load_dotenv 
from kafka import KafkaProducer # For producing messages to a Kafka topic

load_dotenv() 

# --- Configuration ---

API_Key = os.getenv("FINNHUB_API_KEY")
if not API_Key:
    raise ValueError("FINNHUB_API_KEY environment variable not set.")

# Base URL for the Finnhub API quote endpoint
base_url="https://finnhub.io/api/v1/quote"

# List of stock symbols to fetch quotes for
SYMBOLS=["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

# Initialize the Kafka producer.
# It connects to the Kafka broker running on the host machine from within a Docker container.
# The value_serializer encodes the data from a dictionary to a JSON string, then to UTF-8 bytes.
producer = KafkaProducer(bootstrap_servers=["host.docker.internal:29092"],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8')
                         )

def fetch_quote(symbol):
    """
    Fetches the latest stock quote for a given symbol from the Finnhub API.
    
    Args:
        symbol (str): The stock symbol (e.g., "AAPL").
        
    Returns:
        dict: A dictionary containing the quote data, or None if the fetch fails.
    """
    url=f"{base_url}?symbol={symbol}&token={API_Key}"
    try:
        response = requests.get(url)
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        
        data = response.json() # Parse the JSON response
        data["symbol"]= symbol # Add the stock symbol to the data
        data["fetched_at"]=int(time.time()) # Add a Unix timestamp for when the data was fetched
        return data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred for {symbol}: {http_err}")
        return None
    except json.JSONDecodeError:
        print(f"Failed to decode JSON for {symbol}. Response text: {response.text}")
        return None
    
# Main loop to continuously fetch and produce data
while True:
    # Iterate over each symbol in the list
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        # If a quote was successfully fetched, send it to Kafka
        if quote:
            print(f"Producing quote for {symbol}: {quote}")
            # Send the quote data to the 'stock-quotes' topic
            producer.send("stock-quotes", value=quote) 
    # Wait for 6 seconds before fetching the quotes again to respect API rate limits
    time.sleep(6)