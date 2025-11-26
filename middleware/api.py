# middleware/api.py

import socket
import time
from .transport import serialize, deserialize
import middleware.state as st
from .threads import start_threads

# -------------------------------------------------------------------
# GROUP JOIN
# -------------------------------------------------------------------
def grp_join(group_id, discovery_ip, discovery_port, myid):
    print(f"[DEBUG] grp_join() called by '{myid}' for group '{group_id}'")

    # 1️⃣ DISCOVERY → SEND "DISCOVER"
    disc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    disc_sock.settimeout(2.0)

    disc_msg = serialize(["DISCOVER", myid])
    disc_sock.sendto(disc_msg, (discovery_ip, discovery_port))

    try:
        data, src = disc_sock.recvfrom(1024)
        response = deserialize(data)  # e.g. ["DISCOVER_ACK", [50001]]
        print("[DEBUG] DISCOVER_ACK received:", response)
    except Exception:
        print("[ERROR] No discovery response")
        return None

    # STORE MANAGER INFO
    st.manager_address[0] = src[0]          # manager IP
    st.server_port[0]     = response[1][0]  # manager TCP port
    st.id[0]              = myid   
    
    # 2️⃣ START THREADS ONCE
    print("[DEBUG] Starting threads now that manager is known")
    start_threads()

    # 3️⃣ TCP JOIN → **FIX FORMAT HERE**
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_sock.connect((st.manager_address[0], st.server_port[0]))
    print("[DEBUG] TCP connection established!")

    join_msg = ["JOIN", [group_id, myid]]   # CORRECT format ⚡
    tcp_sock.send(serialize(join_msg))
    print(f"[DEBUG] Sent JOIN: {join_msg}")

    # 4️⃣ WAIT FOR JOIN_ACK
    while True:
        data = tcp_sock.recv(1024)
        if not data:
            print("[ERROR] TCP closed while waiting for JOIN_ACK")
            return None

        msg = deserialize(data)
        print("[DEBUG] POLL RESPONSE →", msg)

        if msg[0] == "JOIN_ACK":
            multicast_port = msg[1][0]    # port from server
            members = msg[1][1]           # members list
            st.multicast_port[0] = multicast_port
            st.first_join = 1

            print(f"[DEBUG] JOIN_ACK received → multicast_port = {multicast_port}, members = {members}")
            return multicast_port    # IMPORTANT: return port ONLY!

        time.sleep(0.1)

# -------------------------------------------------------------------
# LEAVE GROUP
# -------------------------------------------------------------------
def grp_leave(group_id, myid):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((st.manager_address[0], st.server_port[0]))
        sock.send(serialize(["LEAVE", group_id, myid]))
        sock.close()
    except Exception as e:
        print("[ERROR] grp_leave =>", e)


# -------------------------------------------------------------------
# SEND APPLICATION MESSAGE
# -------------------------------------------------------------------
def grp_send(sock, msg, flags=0):
    try:
        payload = serialize(["APP", msg])
        sock.send(payload)
    except Exception as e:
        print("[ERROR] grp_send =>", e)


# -------------------------------------------------------------------
# RECV APPLICATION MESSAGE
# (blocking call)
# -------------------------------------------------------------------
def grp_recv(sock, msg_type, rec, size=1024, flags=0):
    try:
        data = sock.recv(size)
        if not data:
            rec[0] = "NaN"
            return
        msg = deserialize(data)
        if msg[0] == msg_type:
            rec[0] = msg[1]
        else:
            rec[0] = "NaN"
    except Exception:
        rec[0] = "NaN"
