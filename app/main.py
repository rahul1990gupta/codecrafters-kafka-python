import socket  # noqa: F401
import struct


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    (clientsocket, address)  = server.accept() # wait for client
    if clientsocket:
        print(f"connect by {address}")
        data = clientsocket.recv(1024)
        if data:
            message_length = struck.pack('>I', 10)
            correlation_id = struct.pack('>I', 7)

            response = message_length + correlation_id
            conn.sendall(response)
            print(f"response sent: {response}")


if __name__ == "__main__":
    main()
