# middleware/utils.py
# -------------------------------------------
# Utility functions used by multiple modules
# -------------------------------------------

import socket


def get_default_ip():
    """
    Detects the preferred local network interface.
    Works on ANY machine (WiFi, ethernet, Docker, lab PCs).
    Does NOT send traffic – just routing lookup.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))  # no actual traffic
        ip = s.getsockname()[0]
        print(f"[DEBUG] Using interface IP: {ip}")
    except Exception:
        ip = "127.0.0.1"
        print("[WARN] Defaulting to 127.0.0.1")
    finally:
        s.close()
    return ip


def debug_raw_sniffer():
    import socket, struct, time
    import middleware.state as st   # <-- MUST IMPORT STATE

    print("[SNIFFER] STARTING RAW SOCKET MULTICAST TEST...")

    # STEP 1 — WAIT until JOIN completed and state exists
    while True:
        st.states_list_mutex.acquire()
        if st.states_list:
            state = st.states_list[0]
            st.states_list_mutex.release()
            break
        st.states_list_mutex.release()
        time.sleep(0.1)

    group_id = state[0]
    multicast_port = state[1]

    print(f"[SNIFFER] GOT STATE for group {group_id} with port {multicast_port}")

    MULTICAST_ADDRESS = "224.51.105.104"

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", multicast_port))

    mreq = struct.pack("=4sl", socket.inet_aton(MULTICAST_ADDRESS), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"[SNIFFER] LISTENING on {MULTICAST_ADDRESS}:{multicast_port}")

    while True:
        data, addr = sock.recvfrom(2048)
        print(f"[SNIFFER] GOT DATA: {data} from {addr}")