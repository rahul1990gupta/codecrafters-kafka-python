import socket  # noqa: F401
import struct


def handle_client(clientsocket):
    with clientsocket:
        data = clientsocket.recv(1024)
        print(data)
        mlen = int.from_bytes(data[:4], byteorder="big") # 4 bytes
        request_api_key = int.from_bytes(data[4:6], byteorder="big") # 2 bytes
        request_api_version = int.from_bytes(data[6:8], byteorder="big") # 2 bytes
        correlation_id = int.from_bytes(data[8:12], byteorder="big") # 4 bytes


        if data:
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

