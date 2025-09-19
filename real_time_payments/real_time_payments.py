# File: kafka_payment_consumer.py
# This runs as a separate service, NOT in Power BI

import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
from importToBI3 import ClearVueBIProcessor
def run_payment_consumer():
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["clearvue"]
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        'payment-transactions',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='clearvue-payments-group'
    )
    
    print("Payment stream consumer started. Listening for messages...")
    
    try:
        for message in consumer:
            payment_data = message.value
            print(f"Received payment: {payment_data}")
            
            # Add timestamp and financial period
            payment_data['processed_at'] = datetime.now()
            if 'DEPOSIT_DATE' in payment_data:
                payment_data['FINANCIAL_PERIOD'] = ClearVueBIProcessor.calculate_financial_period(payment_data['DEPOSIT_DATE'])
            
            # Store in MongoDB
            db.payment_stream.insert_one(payment_data)
            print(f"Payment stored in MongoDB: {payment_data['DEPOSIT_REF']}")
            
    except Exception as e:
        print(f"Error in payment consumer: {e}")
        time.sleep(5)  # Wait before retrying
        run_payment_consumer()  # Restart

if __name__ == "__main__":
    run_payment_consumer()