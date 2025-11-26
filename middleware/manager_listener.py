# middleware/manager_listener.py
import socket
from .transport import serialize, deserialize
import middleware.state as st

def group_manager_listener():
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # 💥 FIX HERE – is string, not tuple!
            sock.connect((st.manager_address[0], st.server_port[0]))

            sock.send(serialize(["POLL", st.id[0]]))
            data = sock.recv(1024)
            sock.close()

            if not data:
                continue

            msg = deserialize(data)
            if msg[0] == "NACK":
                continue

            st.mutex.acquire()
            st.message_list.append(msg)
            st.mutex.release()

            print(f"[DEBUG] POLL RESPONSE → {msg}")

        except Exception as e:
            print(f"[ERROR] Poll exception: {e}")
            continue
