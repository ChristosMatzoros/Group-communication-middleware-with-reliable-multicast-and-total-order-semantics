# middleware/reliability.py

import time
import socket
from .state import send_list, send_list_mutex
from .transport import serialize, deserialize

from middleware.utils import get_default_ip



def multicast_sender(state, id_seqno, msg):
    """Send packet via the SAME socket that joined the multicast group."""
    packet = serialize(["RM-MSG", id_seqno, msg])

    sock = state[2]                                 # <-- SAME SOCKET!
    multicast_addr, group_port = state[9]           # <-- NEW IN STATE
    local_ip = get_default_ip()

    # Important multicast opts
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(local_ip))
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

    print(f"[SEND-DEBUG] → {multicast_addr}:{group_port} via {local_ip} | {msg}")
    sock.sendto(packet, (multicast_addr, group_port))