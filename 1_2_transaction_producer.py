# Financial Transaction Data Producer
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import uuid

class FinancialTransactionProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Sample data for realistic transactions
        self.merchants = [
            'Amazon', 'Walmart', 'Target', 'Starbucks', 'Shell Gas',
            'McDonald\'s', 'Best Buy', 'Home Depot', 'CVS Pharmacy', 'Uber'
        ]
        
        self.transaction_types = ['purchase', 'refund', 'transfer', 'withdrawal', 'deposit']
        
        self.currencies = ['USD', 'EUR', 'GBP', 'CAD', 'AUD']
        
        self.account_ids = [f'ACC{str(i).zfill(6)}' for i in range(1000, 1100)]
    
    def generate_transaction(self):
        """Generate a realistic financial transaction"""
        transaction_id = str(uuid.uuid4())
        account_id = random.choice(self.account_ids)
        
        # Generate realistic amounts based on transaction type
        transaction_type = random.choice(self.transaction_types)
        
        if transaction_type == 'withdrawal':
            amount = round(random.uniform(20, 500), 2)
        elif transaction_type == 'deposit':
            amount = round(random.uniform(100, 5000), 2)
        elif transaction_type == 'transfer':
            amount = round(random.uniform(50, 2000), 2)
        elif transaction_type == 'refund':
            amount = round(random.uniform(10, 300), 2)
        else:  # purchase
            amount = round(random.uniform(5, 800), 2)
        
        # Simulate some late-arriving transactions (5% chance of being 1-10 minutes old)
        if random.random() < 0.05:
            event_time = datetime.now() - timedelta(minutes=random.randint(1, 10))
        else:
            event_time = datetime.now()
        
        transaction = {
            'transaction_id': transaction_id,
            'account_id': account_id,
            'transaction_type': transaction_type,
            'amount': amount,
            'currency': random.choice(self.currencies),
            'merchant': random.choice(self.merchants) if transaction_type == 'purchase' else None,
            'event_time': event_time.isoformat(),
            'processing_date': datetime.now().date().isoformat(),
            'location': {
                'country': random.choice(['US', 'CA', 'UK', 'AU', 'DE']),
                'city': random.choice(['New York', 'Toronto', 'London', 'Sydney', 'Berlin'])
            },
            'channel': random.choice(['online', 'atm', 'pos', 'mobile']),
            'status': random.choice(['pending', 'completed', 'failed']),
            # Add some metadata for processing
            'metadata': {
                'ip_address': f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
                'device_id': f"device_{random.randint(1000, 9999)}",
                'session_id': f"session_{random.randint(10000, 99999)}"
            }
        }
        
        return transaction
    
    def send_transactions(self, count=None, continuous=False):
        """Send transactions to Kafka"""
        if continuous:
            print("Starting continuous transaction stream... (Press Ctrl+C to stop)")
            try:
                while True:
                    transaction = self.generate_transaction()
                    
                    # Use account_id as partition key for better distribution
                    self.producer.send(
                        'transactions',
                        key=transaction['account_id'],
                        value=transaction
                    )
                    
                    print(f"Sent: {transaction['transaction_type']} ${transaction['amount']} "
                          f"from {transaction['account_id']}")
                    
                    # Random delay between 0.1 to 2 seconds
                    time.sleep(random.uniform(0.1, 2.0))
                    
            except KeyboardInterrupt:
                print("\nStopping transaction stream...")
        
        else:
            print(f"Sending {count} transactions...")
            for i in range(count):
                transaction = self.generate_transaction()
                
                self.producer.send(
                    'transactions',
                    key=transaction['account_id'],
                    value=transaction
                )
                
                if i % 10 == 0:
                    print(f"Sent {i} transactions...")
                
                time.sleep(0.1)  # Small delay
            
            print(f"Completed sending {count} transactions")
    
    def close(self):
        """Close the producer"""
        self.producer.flush()
        self.producer.close()

# Usage examples
if __name__ == "__main__":
    producer = FinancialTransactionProducer()
    
    try:
        # Send 100 transactions quickly for testing
        producer.send_transactions(count=100)
        
        # Or run continuously (uncomment next line)
        # producer.send_transactions(continuous=True)
        
    finally:
        producer.close()