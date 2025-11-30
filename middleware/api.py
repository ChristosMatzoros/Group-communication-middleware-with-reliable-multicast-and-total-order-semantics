# middleware/api.py
# ----------------------------------------------------------
# GROUP JOIN, SEND, RECV, LEAVE (API exposed to client)
# ----------------------------------------------------------

import socket
from threading import Thread
import time
import struct
from .transport import serialize, deserialize
import middleware.state as st
from .threads import start_threads
from middleware.state import (
    states_list, send_list, send_list_mutex,
    states_list_mutex, MULTICAST_ADDRESS
)

from middleware.utils import get_default_ip

# ----------------------------------------------------------
# GROUP JOIN
# ----------------------------------------------------------
def grp_join(group_id, discovery_ip, discovery_port, myid):
    """
    Join a multicast group via the Manager:
    1. Discover the manager via UDP.
    2. Start middleware threads (only once).
    3. Connect via TCP to request JOIN.
    4. Setup multicast receiver socket.
    5. Store full group state.
    """

    print(f"[DEBUG] grp_join() called by '{myid}' for group '{group_id}'")

    # ----------------------------------------------------------
    # 1) DISCOVER MANAGER (UDP)
    # ----------------------------------------------------------
    disc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    disc_sock.settimeout(2.0)
    disc_msg = serialize(["DISCOVER", myid])
    disc_sock.sendto(disc_msg, (discovery_ip, discovery_port))

    try:
        data, src = disc_sock.recvfrom(1024)
        response = deserialize(data)
        print("[DEBUG] DISCOVER_ACK received:", response)
    except Exception:
        print("[ERROR] No discovery response")
        return None
    finally:
        disc_sock.close()

    # Always update manager info (essential when joining multiple groups)
    st.manager_address[0] = src[0]
    st.server_port[0]     = response[1][0]
    st.id[0]              = myid

    # ----------------------------------------------------------
    # 2) START THREADS — only once
    # ----------------------------------------------------------
    if not hasattr(st, "threads_started"):
        st.threads_started = [False]

    if not st.threads_started[0]:
        print("[DEBUG] Starting threads now that manager is known")
        start_threads()
        st.threads_started[0] = True
    else:
        print("[DEBUG] Threads already running – not starting again")

    # ----------------------------------------------------------
    # 3) TCP CONNECT AND SEND JOIN REQUEST
    # ----------------------------------------------------------
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 💥 NEW: GUARANTEE VALID MANAGER INFO
    if not st.manager_address[0] or not st.server_port[0]:
        print(f"[ERROR] manager info invalid → {st.manager_address[0]}:{st.server_port[0]}")
        print("[DEBUG] Retrying discovery...")
        return grp_join(group_id, discovery_ip, discovery_port, myid)   # retry safely

    try:
        tcp_sock.connect((st.manager_address[0], st.server_port[0]))
    except OSError as e:
        print(f"[ERROR] TCP connection failed: {e}")
        time.sleep(0.5)
        return grp_join(group_id, discovery_ip, discovery_port, myid)  # automatic retry

    print("[DEBUG] TCP connection established!")

    tcp_sock.send(serialize(["JOIN", [group_id, myid]]))
    print(f"[DEBUG] Sent JOIN: ['JOIN', ['{group_id}', '{myid}']]")

    # Wait for JOIN_ACK
    while True:
        data = tcp_sock.recv(1024)
        if not data:
            print("[ERROR] TCP closed while waiting for JOIN_ACK")
            return None

        msg = deserialize(data)
        if msg[0] == "JOIN_ACK":
            multicast_port = msg[1][0]
            members        = msg[1][1]
            break
    tcp_sock.close()

    print(f"[DEBUG] JOIN_ACK → port={multicast_port}, members = {members}")

    # ----------------------------------------------------------
    # 4) CREATE MULTICAST RECEIVER SOCKET
    # ----------------------------------------------------------
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # BEST PRACTICE → bind to ALL interfaces
    sock.bind(("", multicast_port))   # not local_ip

    # CORRECT MULTICAST JOIN — THIS WORKS ON ALL PCs
    mreq = struct.pack("=4sl", 
        socket.inet_aton(MULTICAST_ADDRESS), 
        socket.INADDR_ANY           # <--- FIXED
    )
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"[DEBUG] JOINED MULTICAST GROUP {MULTICAST_ADDRESS}:{multicast_port}")

    # ----------------------------------------------------------
    # 5) STORE GROUP STATE
    # ----------------------------------------------------------
    st.states_list_mutex.acquire()
    st.states_list.append([
        group_id,            # 0
        multicast_port,      # 1
        sock,                # 2  (same socket for recv/send)
        0,                   # 3 local seq number
        members,             # 4 known members
        [],                  # 5 seen message ids
        0,                   # 6 last_delivered_seq
        [],                  # 7 pending seqs + msg content
        0,                   # 8 sequencer counter
        (MULTICAST_ADDRESS, multicast_port)  # 9 (addr, port)
    ])
    st.states_list_mutex.release()

    return sock



# ----------------------------------------------------------
# SEND MESSAGE TO GROUP
# ----------------------------------------------------------
def grp_send(gsock, msg, flags=0):

    states_list_mutex.acquire()
    target_state = None
    for state in states_list:
        if state[2] == gsock or state[1] == gsock:
            target_state = state
            break
    states_list_mutex.release()

    if target_state is None:
        print(f"[ERROR] grp_send: group socket {gsock} not found!")
        return

    # Increase local seq number
    states_list_mutex.acquire()
    target_state[3] += 1
    seqno = target_state[3]
    states_list_mutex.release()

    packet = ["TRM-MSG", [st.id[0], seqno], msg]

    # Push into send_list → middleware thread will handle multicast
    send_list_mutex.acquire()
    send_list.append([target_state, [st.id[0], seqno], packet])
    send_list_mutex.release()


# ----------------------------------------------------------
# RECV MESSAGE (not used anymore - total order takes over)
# ----------------------------------------------------------
def grp_recv(sock, msg_type, rec, size=1024, flags=0):
    try:
        data = sock.recv(size)
        if not data:
            rec[0] = "NaN"
            return
        msg = deserialize(data)
        rec[0] = msg[1] if msg[0] == msg_type else "NaN"
    except:
        rec[0] = "NaN"


# ----------------------------------------------------------
# LEAVE GROUP
# ----------------------------------------------------------
def grp_leave(group_id, myid):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((st.manager_address[0], st.server_port[0]))
        sock.send(serialize(["LEAVE", group_id, myid]))
        sock.close()
    except Exception as e:
        print("[ERROR] grp_leave =>", e)
