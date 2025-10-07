import socket
s = socket.create_connection(("localhost", 29092))
print("✅ Connected to Kafka")
