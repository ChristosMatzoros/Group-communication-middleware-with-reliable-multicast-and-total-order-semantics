import socket
import time
from .transport import serialize, deserialize

def multicast_sender(state, id_seqno, msg):
    packet = serialize(["RM-MSG", id_seqno, msg])
    list_len = len(state[4])    # number of members

    while True:
        app_send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        app_send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        app_send_sock.settimeout(10)

        app_send_sock.sendto(packet, ("224.51.105.104", state[1]))

        ack_counter = 0
        start = time.time()

        while ack_counter < list_len:
            try:
                ack_msg, _ = app_send_sock.recvfrom(1024)
            except socket.timeout:
                break

            if deserialize(ack_msg) == "ACK":
                ack_counter += 1

        app_send_sock.close()
        if ack_counter == list_len:
            break
