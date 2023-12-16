import socket
import threading
from datetime import datetime
import json
from profanity import profanity
from confluent_kafka import Producer


PORT = 5050
#SERVER = socket.gethostbyname(socket.gethostname())
SERVER = "192.168.53.196"
ADDR = (SERVER, PORT)
HEADER = 64
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
conf = {'bootstrap.servers': '192.168.53.196:9092', 'client.id': socket.gethostname()}
producer = Producer(conf)
CHAT = "chats"
ACTIVITY = "activity"
clients = {}

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)


def handleClient(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected")
    producer.produce(ACTIVITY, key="activities", value=json.dumps({
        "date": str(datetime.now()),
        "user": str(addr),
        "payload": "login"
    }))
    connected = True
    while connected:
        try:
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if(msg_length):
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)
                if profanity.contains_profanity(msg):
                    producer.produce(ACTIVITY, key="activities",value=json.dumps({
                        "date": str(datetime.now()),
                        "user": str(addr),
                        "payload": f"Usage of profanity -----> {msg}"
                    }))
                    conn.send("[SERVER] -------> Usage of profanity is strictly prohibited".encode(FORMAT))
                    continue
                for client in clients:
                    if(conn == client):
                        conn.send("[SERVER] -----> Message Received Successfully!".encode(FORMAT))
                    else:
                        client.send((f"Message from {clients[client]} ----> {msg}").encode(FORMAT))
                if(msg == DISCONNECT_MESSAGE):
                    producer.produce(ACTIVITY, key="activities", value=json.dumps({
                    "date": str(datetime.now()),
                    "user": str(addr),
                    "payload": "logout"
                }))

                    del clients[conn]
                    connected = False
                    print(f"[DISCONNECTING] ----> {addr}")
                    continue

                producer.produce(CHAT, key="messages", value=json.dumps({
                    "date": str(datetime.now()),
                    "user": str(addr),
                    "payload": msg
                }))

                print(f"[NEW MESSAGE] FROM: {addr} ----> {msg}")

        except KeyboardInterrupt:
            connected = False
            print(f"[DISCONNECTING] ----> {addr}")
            producer.produce(ACTIVITY, key="activities", value=json.dumps({
                "date": str(datetime.now()),
                "user": str(addr),
                "payload": "logout"
            }))
            del clients[conn]


    conn.close()
    print(f"[DISCONNECTED] {addr}")

def start():
    server.listen()
    producer.produce(ACTIVITY, key="activities", value=json.dumps({
    "date": str(datetime.now()),
    "SERVER": str(SERVER),
    "payload": "STARTING"
    }))
    print(f"[LISTENING] ----> {SERVER}")
    while True:
        conn, addr = server.accept()
        clients[conn] = str(addr)
        thread = threading.Thread(target=handleClient, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE] {threading.active_count() - 1}")


try:
    print("[STARTING] server is starting")
    start()

except KeyboardInterrupt:
    producer.produce(ACTIVITY, key="activities", value=json.dumps({
        "date": str(datetime.now()),
        "SERVER": str(SERVER),
        "payload": "STOPPING"
    }))
    producer.flush()
