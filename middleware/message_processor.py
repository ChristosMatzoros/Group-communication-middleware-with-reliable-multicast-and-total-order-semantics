# middleware/message_processor.py

import copy
import middleware.state as st
from middleware.reliability import multicast_sender
import time

def application_message_processor():
    while True:
        time.sleep(0.01)
        st.packets_list_mutex.acquire()
        if not st.packets_list:
            st.packets_list_mutex.release()
            continue

        packet = st.packets_list.pop(0)
        st.packets_list_mutex.release()

        msg = copy.copy(packet[2])    # TRM-MSG or TRM-SEQ
        pid_k = copy.copy(packet[1])  # ['Chris', 1]
        group = copy.copy(packet[3][0])  # CORRECT group lookup

        # ----------------- FIND STATE -----------------
        st.states_list_mutex.acquire()
        state = None
        for s in st.states_list:
            if s[0] == group:
                state = s
                break
        st.states_list_mutex.release()
        if state is None:
            continue

        curr_pid = copy.copy(pid_k)
        if len(curr_pid) == 3:
            curr_pid.remove("SEQ")

        # ----------------- DUPLICATE -----------------
        found = False
        st.states_list_mutex.acquire()
        for msgid in state[5]:
            if msgid == curr_pid:
                found = True
                break
        if not found:
            state[5].append(curr_pid)
        st.states_list_mutex.release()

        if found:
            continue

        # ----------------- RESEND IF NOT ORIGINAL SENDER -----------------
        if pid_k[0] != st.id[0] and msg[0] == "TRM-MSG":
            multicast_sender(state, pid_k, msg)

        # ----------------- SEQUENCER CASE -----------------
        is_seq = (state[4][0] == st.id[0])   # if I'm the first member
        if is_seq and msg[0] == "TRM-MSG":
            st.states_list_mutex.acquire()
            state[8] += 1
            order_no = state[8]
            st.states_list_mutex.release()

            msg[1].append("SEQ")
            seq_packet = ["TRM-SEQ", msg[1], order_no]
            multicast_sender(state, msg[1], seq_packet)

            # SENDER should also see the message in GREEN:
            payload_only = msg[2]   # safe when sequencer
            st.application_messages_list_mutex.acquire()
            st.application_messages_list.append(["APP", payload_only, state[1]])
            st.application_messages_list_mutex.release()

            continue   # 🚨 THIS SAVES YOU FROM NON-SEQUENCER CODE BELOW

        # ----------------- NON-SEQUENCER: WAIT -----------------

        st.states_list_mutex.acquire()
        found = False
        for item in state[7]:
            if item[0] == msg[1]:
                if msg[0] == "TRM-MSG":
                    item[2] = msg[2]
                elif msg[0] == "TRM-SEQ":
                    item[1] = msg[2]
                found = True
                break

        if not found:
            if msg[0] == "TRM-MSG":
                state[7].append([msg[1], -1, msg[2]])
            elif msg[0] == "TRM-SEQ":
                state[7].append([msg[1], msg[2], ""])
        st.states_list_mutex.release()

        # try deliver in order
        while True:
            st.states_list_mutex.acquire()
            delivered = False
            for item in state[7]:
                if item[1] != -1 and item[2] != "" and item[1] == state[6] + 1:
                    state[7].remove(item)
                    state[6] += 1

                    # extract correct payload
                    payload_only = item[2]
                    if isinstance(payload_only, list) and len(payload_only) == 3:
                        payload_only = payload_only[2]

                    # append safely
                    st.application_messages_list_mutex.acquire()
                    st.application_messages_list.append(["APP", payload_only, state[1]])
                    st.application_messages_list_mutex.release()

                    delivered = True
                    break
            st.states_list_mutex.release()
            if not delivered:
                break
