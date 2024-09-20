import socket  # noqa: F401
import struct


def handle_client(clientsocket):
    with clientsocket:
        data = clientsocket.recv(1024)
        print(data)
        if data:
            correlation_id = int(7).to_bytes(4, byteorder="big")
            message_length = len(correlation_id).to_bytes(4, byteorder="big")

            response = message_length + correlation_id
            clientsocket.sendall(response)
            print(f"response sent: {response}")



def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        (clientsocket, address)  = server.accept() # wait for client
        if clientsocket:
            print(f"connect by {address}")
            handle_client(clientsocket)


if __name__ == "__main__":
    main()

