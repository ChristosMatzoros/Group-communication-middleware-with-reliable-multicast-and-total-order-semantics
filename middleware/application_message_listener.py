# middleware/application_message_listener.py
import socket, struct, time
import middleware.state as st
from .transport import deserialize
from middleware.utils import get_default_ip

MULTICAST_ADDRESS = "224.51.105.104"

def application_message_listener():
    print("[DEBUG] application_message_listener STARTED")

    # wait until state exists
    while True:
        st.states_list_mutex.acquire()
        if st.states_list:
            st.states_list_mutex.release()
            break
        st.states_list_mutex.release()
        time.sleep(0.05)

    while True:
        st.states_list_mutex.acquire()
        states_snapshot = list(st.states_list)   # <-- safe copy
        st.states_list_mutex.release()

        for state in states_snapshot:
            sock = state[2]
            try:
                sock.settimeout(0.2)
                data, addr = sock.recvfrom(2048)
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[RECV-ERROR] → {e}")
                continue

            print(f"[RECV] GOT RAW DATA → {data} from {addr}")
            packet = deserialize(data)
            print(f"[RECV] DESERIALIZED → {packet}")

            #  IMPORTANT - SEND TO PROCESSOR!!
            packet.append(state)
            st.packets_list_mutex.acquire()
            st.packets_list.append(packet)
            st.packets_list_mutex.release()