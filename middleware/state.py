# middleware/state.py
import socket
import threading

MULTICAST_ADDRESS = "224.51.105.104"

# these MUST be mutable lists so they can be updated inside functions
first_join     = [0]
manager_address = [None]
server_port     = [None]
id              = [""]

# multicast_port MUST be here:
multicast_port  = [None]  # 🟢 ADDED — this was missing before!

tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

message_list = []
states_list = []
packets_list = []
application_messages_list = []
msgids_list = []
send_list = []

# Locks
mutex = threading.Lock()
states_list_mutex = threading.Lock()
application_messages_list_mutex = threading.Lock()
packets_list_mutex = threading.Lock()
send_list_mutex = threading.Lock()
