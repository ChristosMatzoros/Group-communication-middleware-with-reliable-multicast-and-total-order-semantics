# middleware/message_processor.py

import copy
import time
import middleware.state as st
from middleware.reliability import multicast_sender


def application_message_processor():
    while True:
        time.sleep(0.01)

        # -----------------------
        # FETCH NEXT PACKET
        # -----------------------
        st.packets_list_mutex.acquire()
        if not st.packets_list:
            st.packets_list_mutex.release()
            continue

        packet = st.packets_list.pop(0)
        st.packets_list_mutex.release()

        msg     = packet[2]          # TRM-MSG or TRM-SEQ
        pid_k   = packet[1]          # ['sender', seqno] or ['sender', seqno, 'SEQ']
        state   = packet[3]          # the state object passed by listener
        group   = state[0]

        sender  = pid_k[0]
        seqno   = pid_k[1]
        mtype   = msg[0]             # "TRM-MSG" or "TRM-SEQ"

        # UNIQUE message id (fix for duplicates)
        msg_id = (sender, seqno, mtype)

        # -----------------------
        # DUPLICATE FILTER
        # -----------------------
        st.states_list_mutex.acquire()
        if msg_id in state[5]:
            st.states_list_mutex.release()
            continue
        state[5].append(msg_id)
        st.states_list_mutex.release()

        # -----------------------
        # REBROADCAST (gossip)
        # -----------------------
        if sender != st.id[0] and mtype == "TRM-MSG":
            multicast_sender(state, pid_k, msg)

        # -----------------------
        # SEQUENCER LOGIC
        # -----------------------
        is_sequencer = (state[4][0] == st.id[0])

        if is_sequencer and mtype == "TRM-MSG":
            # assign sequence number
            st.states_list_mutex.acquire()
            state[8] += 1
            order_no = state[8]
            st.states_list_mutex.release()

            # broadcast TRM-SEQ
            seq_pid = [sender, seqno, "SEQ"]
            seq_pkt = ["TRM-SEQ", seq_pid, order_no]
            multicast_sender(state, seq_pid, seq_pkt)

            # deliver to APP immediately (green)
            st.application_messages_list_mutex.acquire()
            st.application_messages_list.append(["APP", msg[2], state[1]])
            st.application_messages_list_mutex.release()

            continue  # don't execute non-sequencer path

        # -------------------------------------------------
        # NON-SEQUENCER: store MSG + SEQ in pending buffer
        # -------------------------------------------------
        st.states_list_mutex.acquire()
        pending = state[7]
        found = False

        for item in pending:
            if item[0][0] == sender and item[0][1] == seqno:
                # existing entry
                if mtype == "TRM-MSG":
                    item[2] = msg[2]      # payload text
                else:  # TRM-SEQ
                    item[1] = msg[2]      # global order number
                found = True
                break

        if not found:
            if mtype == "TRM-MSG":
                pending.append([[sender, seqno], -1, msg[2]])
            else:
                pending.append([[sender, seqno], msg[2], ""])

        st.states_list_mutex.release()

        # -------------------------------------------------
        # TRY DELIVERY (total order)
        # -------------------------------------------------
        while True:
            st.states_list_mutex.acquire()
            delivered = False
            for item in list(pending):
                pid, order_no, payload = item

                if order_no != -1 and payload != "" and order_no == state[6] + 1:
                    # deliver now
                    pending.remove(item)
                    state[6] += 1

                    st.application_messages_list_mutex.acquire()
                    st.application_messages_list.append(["APP", payload, state[1]])
                    st.application_messages_list_mutex.release()

                    delivered = True
                    break

            st.states_list_mutex.release()

            if not delivered:
                break
