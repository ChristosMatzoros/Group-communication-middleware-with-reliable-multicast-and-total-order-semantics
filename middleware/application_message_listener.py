# middleware/application_message_listener.py

import socket
import struct
import time
from .transport import deserialize
import middleware.state as st

MULTICAST_ADDRESS = "224.51.105.104"


def application_message_listener():
    """
    Listens on the multicast socket for group messages and pushes them
    into packets_list to be processed by application_message_processor.
    """

    # 🟢 WAIT until grp_join has set multicast_port[0]
    while st.multicast_port[0] is None:
        print("[DEBUG] application_message_listener waiting for multicast_port...")
        time.sleep(0.1)

    port = st.multicast_port[0]

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", port))

    # Join multicast group
    mreq = struct.pack("=4sl", socket.inet_aton(MULTICAST_ADDRESS), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"[DEBUG] Listening for multicast on {MULTICAST_ADDRESS}:{port}")

    while True:
        try:
            data, addr = sock.recvfrom(1024)
            msg = deserialize(data)

            st.packets_list_mutex.acquire()
            st.packets_list.append(msg)
            st.packets_list_mutex.release()

            print(f"[DEBUG] Multicast received from {addr} → {msg}")

        except Exception as e:
            print(f"[ERROR] multicast listener exception: {e}")
            continue
