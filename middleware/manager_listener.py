# middleware/manager_listener.py

import socket
from .transport import serialize, deserialize
import middleware.state as st
import copy

def group_manager_listener():
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # TCP connect to manager
        while True:
            try:
                sock.close()
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((st.manager_address[0], st.server_port[0]))
            except:
                continue
            break

        sock.send(serialize(["POLL", st.id[0]]))
        data = sock.recv(1024)
        sock.close()

        if not data:
            continue

        msg = deserialize(data)
        if msg[0] == "NACK":
            continue

        # ------------ NEW_MEMBER -------------
        if msg[0] == "NEW_MEMBER":
            group = msg[1][0]
            new_id = msg[1][1]

            st.states_list_mutex.acquire()
            cur_state = None
            for s in st.states_list:
                if s[0] == group:
                    s[4].append(new_id)
                    cur_state = copy.copy(s)
                    break
            st.states_list_mutex.release()

            st.application_messages_list_mutex.acquire()
            st.application_messages_list.append(
                ["GROUP", f"{new_id} has joined group {group}.", cur_state[1]]
            )
            st.application_messages_list_mutex.release()

            # ACK to manager
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((st.manager_address[0], st.server_port[0]))
            sock.send(serialize(["NEW_MEMBER_ACK"]))
            sock.close()
            continue

        # ------------ LEAVE_MEMBER ------------
        if msg[0] == "LEAVE_MEMBER":
            group = msg[1][0]
            member = msg[1][1]
            seq_s  = msg[1][2]

            st.states_list_mutex.acquire()
            cur_state = None
            for s in st.states_list:
                if s[0] == group:
                    s[8] = seq_s
                    s[4].remove(member)
                    cur_state = copy.copy(s)
                    break
            st.states_list_mutex.release()

            st.application_messages_list_mutex.acquire()
            st.application_messages_list.append(
                ["GROUP", f"{member} has left group {group}.", cur_state[1]]
            )
            st.application_messages_list_mutex.release()

            # ACK to manager
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((st.manager_address[0], st.server_port[0]))
            sock.send(serialize(["LEAVE_MEMBER_ACK"]))
            sock.close()
            continue
