# manager/server.py
# ------------------------------------------------------------
# Group Manager: handles DISCOVER, JOIN, and LEAVE
# ------------------------------------------------------------

import socket
import struct
import pickle
import threading
import sys
import signal
from shared.color import Color
from middleware.utils import get_default_ip  

# -----------------------------
# GLOBAL STATE
# -----------------------------
groups_list = []     # each element = [groupid, multicast_port, [members]]
members_list = []    # each element = [member_id, ip, udp_port]
message_list = []

stream_server_host = '127.0.0.1'
stream_client_port = int(sys.argv[1])  # TCP port for manager

join_sem = threading.Semaphore(0)
leave_sem = threading.Semaphore(0)

message_list_mutex = threading.Lock()
members_list_mutex = threading.Lock()

multicast_port = 1030
gsock = 0

# -----------------------------
# SERIALIZE
# -----------------------------
def serialize(data):
    return pickle.dumps(data)

def deserialize(data):
    return pickle.loads(data)


# -----------------------------
# SIGNAL HANDLER
# -----------------------------
def signal_handler(sig, frame):
    print(Color.F_Red + "[SERVER] Gracefully shutting down." + Color.F_Default)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


# -------------------------------------------------------------
# UDP DISCOVERY SOCKET
# -------------------------------------------------------------
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udp_socket.bind(("", 4242))

local_ip = get_default_ip()
print(f"[DEBUG] Manager using interface IP: {local_ip}")

# 🔥 FIX: multicast group JOIN – ALWAYS use `4sl` and `INADDR_ANY`
mreq = struct.pack(
    "=4sl", 
    socket.inet_aton("224.51.105.104"), 
    socket.INADDR_ANY
)
udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

# -----------------------------------------------------------
# UDP Discovery Listener  → Handles ["DISCOVER", myid]
# -----------------------------------------------------------
def udp_listener():
    while True:
        msg, (client_ip, client_port) = udp_socket.recvfrom(1024)
        response = deserialize(msg)  # expect ["DISCOVER", myid]

        if not isinstance(response, list) or len(response) < 2:
            continue

        myid = str(response[1])

        members_list_mutex.acquire()
        members_list.append([myid, client_ip, client_port])
        members_list_mutex.release()

        print(Color.F_Cyan + f"[DEBUG] New member discovered: {myid}" + Color.F_Default)

        # Respond with ["DISCOVER_ACK", [tcp_port]]
        udp_socket.sendto(
            serialize(["DISCOVER_ACK", [stream_client_port]]),
            (client_ip, client_port)
        )


# -----------------------------------------------------------
# TCP Listener  → JOIN & LEAVE requests
# -----------------------------------------------------------
def tcp_listener():
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_socket.bind(('', stream_client_port))
    tcp_socket.listen(10)

    print(Color.F_Green + f"[SERVER STARTED] Listening on TCP port {stream_client_port}." + Color.F_Default)

    while True:
        conn, (client_ip, client_port) = tcp_socket.accept()
        data = conn.recv(1024)

        if not data:
            conn.close()
            continue

        msg = deserialize(data)

        tag = msg[0]

        # -----------------------------------------------------------
        # POLL handler → NO verbose printing
        # -----------------------------------------------------------
        if tag == "POLL":
            conn.send(serialize(["NACK", []]))
            conn.close()
            continue

        # -----------------------------------------------------------
        # JOIN handler
        # -----------------------------------------------------------
        if tag == "JOIN":
            handle_join(conn, msg)
            continue

        # -----------------------------------------------------------
        # LEAVE handler (TODO)
        # -----------------------------------------------------------
        if tag == "LEAVE":
            handle_leave(conn, msg)
            continue

        # Unknown
        print(Color.F_Red + f"[ERROR] Unknown TCP message: {msg}" + Color.F_Default)
        conn.close()



# -----------------------------------------------------------
# JOIN HANDLER  (Critical FIX here!)
# -----------------------------------------------------------
def handle_join(conn, msg):
    global multicast_port

    # msg = ["JOIN", ["1", "Chris"]]
    if len(msg) < 2 or not isinstance(msg[1], list) or len(msg[1]) < 2:
        print(Color.F_Red + "[ERROR] JOIN message malformed =>", msg + Color.F_Default)
        conn.close()
        return

    group_id = str(msg[1][0])
    myid     = str(msg[1][1])

    print(Color.F_Yellow + f"[DEBUG] JOIN requested => group={group_id}, user={myid}" + Color.F_Default)

    group_exists = False
    members_in_group = []

    # ----- Check for existing group -----
    for group in groups_list:
        if group[0] == group_id:
            gsock = group[1]
            group[2].append(myid)
            members_in_group = group[2]
            group_exists = True
            break

    # ----- Otherwise, create a new group -----
    if not group_exists:
        multicast_port += 1
        gsock = multicast_port
        groups_list.append([group_id, gsock, [myid]])
        members_in_group = [myid]
        print(Color.F_Magenta + f"[DEBUG] NEW group created: {group_id} @ port={gsock}" + Color.F_Default)

    # 🔥 CRITICAL: Always send JOIN_ACK over this TCP connection!
    reply = ["JOIN_ACK", [gsock, members_in_group]]
    conn.send(serialize(reply))
    print(Color.F_Green + f"[DEBUG] Sent JOIN_ACK → {reply}" + Color.F_Default)
    conn.close()


# -----------------------------------------------------------
# LEAVE HANDLER
# -----------------------------------------------------------
def handle_leave(conn, msg):
    # TODO: properly implement leave protocol here
    conn.close()


# -----------------------------------------------------------
# START THREADS
# -----------------------------------------------------------
udp_thread = threading.Thread(target=udp_listener, daemon=True)
tcp_thread = threading.Thread(target=tcp_listener, daemon=True)

udp_thread.start()
tcp_thread.start()

# Server thread waits...
udp_thread.join()
tcp_thread.join()
