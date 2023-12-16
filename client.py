import socket
import threading

PORT = 5050
HEADER = 64
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
SERVER = "192.168.53.196"
ADDR = (SERVER, PORT)

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR)
connected = [True]

def send(client, connected):
    while connected[0]:
        msg = input(": ")
        if(msg):
            message = msg.encode(FORMAT)
            msg_length = len(message)
            send_length = str(msg_length).encode(FORMAT)
            send_length += b' ' * (HEADER - len(send_length))
            client.send(send_length)
            client.send(message)
            if(msg == DISCONNECT_MESSAGE):
                connected[0] = False
        else:
            print("[INVALID MESSAGE]")


def listen(client, connected):
    while connected[0]:
        print(f"{client.recv(1024).decode(FORMAT)}")

listen_thread = threading.Thread(target=listen, args=(client, connected,))
listen_thread.start()
send_thread = threading.Thread(target=send, args=(client, connected,))
send_thread.start()

send_thread.join()
listen_thread.join()

print("[QUITING CHAT]")
