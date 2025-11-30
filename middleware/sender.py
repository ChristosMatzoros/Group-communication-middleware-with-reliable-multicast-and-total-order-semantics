import time          # 🔥 ADD THIS
import socket
from .state import send_list, send_list_mutex
from .reliability import multicast_sender

def application_sender():
    while True:
        send_list_mutex.acquire()
        if not send_list:
            send_list_mutex.release()
            time.sleep(0.1)  # 🟢 avoid CPU 100%
            continue

        packet = send_list.pop(0)
        send_list_mutex.release()

        print(f"[DEBUG] SENDING PACKET → {packet}")  # 🟢 confirm send
        multicast_sender(packet[0], packet[1], packet[2])
