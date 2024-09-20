import socket  # noqa: F401
import struct
import threading


SUPPORTED_VERSIONS = {0, 1, 2, 3, 4}
ERROR_CODE_UNSUPPORTED_VERSION = 35  # Error code for unsupported version

def prepare_header(correlation_id):
    pass


def prepare_body_for_apiversion(request_api_version):
    response = b""

    if request_api_version not in SUPPORTED_VERSIONS:
        response += ERROR_CODE_UNSUPPORTED_VERSION.to_bytes(2, byteorder="big")
    else: 
        response += int(0).to_bytes(2, byteorder="big")
    
    print(f"response: {response}") 
    response += int(3).to_bytes(1, byteorder="big") # num api keys
    response += int(18).to_bytes(2, byteorder="big")
    response += int(4).to_bytes(2, byteorder="big")
    response += int(4).to_bytes(2, byteorder="big")
    response += int(0).to_bytes(1, byteorder="big") # tag buffer

    response += int(1).to_bytes(2, byteorder="big")
    response += int(16).to_bytes(2, byteorder="big")
    response += int(16).to_bytes(2, byteorder="big")
    response += int(0).to_bytes(1, byteorder="big") # tag buffer
    
    response += int(0).to_bytes(4, byteorder="big") # throttle
    response += int(0).to_bytes(1, byteorder="big") # tag buffer
    
    return response

"""
Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] TAG_BUFFER 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] TAG_BUFFER 
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset TAG_BUFFER 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
"""


def prepare_body_for_fetch(request_api_version):
    body = b""
    body += int(0).to_bytes(4, byteorder="big") # throttle
    body += int(0).to_bytes(2, byteorder="big") # error code 
    body += int(0).to_bytes(4, byteorder="big") # session_id  
    body += int(0).to_bytes(4, byteorder="big") # num responses  
    body += int(0).to_bytes(1, byteorder="big") # tag buffer

    return body


def handle_client(clientsocket, addr):
    while True:
        data = clientsocket.recv(1024)
        print(data)
        mlen = int.from_bytes(data[:4], byteorder="big") # 4 bytes
        request_api_key = int.from_bytes(data[4:6], byteorder="big") # 2 bytes
        request_api_version = int.from_bytes(data[6:8], byteorder="big") # 2 bytes
        correlation_id = int.from_bytes(data[8:12], byteorder="big") # 4 bytes
    
        print(f"message_length: {mlen} \n request_api_key: {request_api_key} \n" +
                f"request_api_version: {request_api_version}\n" +
                f"correlation_id: {correlation_id}")
        
        header = correlation_id.to_bytes(4, byteorder="big")
        if request_api_key ==1: # fetch
            header += int(0).to_bytes(1, byteorder="big")

        body = b""
        
        if request_api_key == 18: # api version
            body = prepare_body_for_apiversion(request_api_version)
        if request_api_key == 1: # fetch
            body = prepare_body_for_fetch(request_api_version)
        
        print(f"header: {header}")

        response = header + body # prepare_body_for_fetch(request_api_version)


        print(f"response: {response}") 
        message_length = len(response).to_bytes(4, byteorder="big")
        response = message_length + response
        print(f"response sent: {response}")
        clientsocket.sendall(response)


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
            client_thread = threading.Thread(target=handle_client, args=(clientsocket, address))
            client_thread.start()  # Start the thread

if __name__ == "__main__":
    main()

