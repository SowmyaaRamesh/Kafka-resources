from kafka import KafkaConsumer

# auto_offset_reset parameter is important. Without it, no results

consumer = KafkaConsumer('TestTopic',bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest')
# print(consumer)

print("The messages read by the consumer are")
for msg in consumer:
    print(msg.value.decode('utf-8'))

# print("hello")