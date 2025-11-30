import sys
import threading
import time

from middleware.api import grp_join, grp_leave, grp_send
from shared.color import Color
import middleware.state as st

# -----------------------------
# CONFIG
# -----------------------------
myid = sys.argv[1]

DISCOVERY_IP   = "224.51.105.104"
DISCOVERY_PORT = 4242

list_of_groups = []
list_of_groups_mutex = threading.Lock()

print(
    Color.F_LightYellow
    + "*** Commands ***\nJOIN <group>\nSEND <group> <msg>\nLEAVE <group>\nEXIT\n"
    + Color.F_Default
)

# -----------------------------
# APP MESSAGE PRINTER
# -----------------------------
def app_printer():
    """
    Continuously prints messages that the middleware has delivered
    into st.application_messages_list.

    Each entry is of the form:
        ["APP" or "GROUP", payload, group_port]
    """
    while True:
        st.application_messages_list_mutex.acquire()
        if st.application_messages_list:
            tag, payload, gport = st.application_messages_list.pop(0)
            st.application_messages_list_mutex.release()

            # For now we just show the group "port" as the group identifier,
            # because that's what the middleware stores (state[1]).
            # This matches what you already saw: [1031] HELLO
            if tag == "GROUP":
                # System / membership messages (joins/leaves)
                print(
                    Color.F_Cyan
                    + f"[{gport}] {payload}"
                    + Color.F_Default
                )
            else:  # "APP"
                print(
                    Color.F_Green
                    + f"[{gport}] {payload}"
                    + Color.F_Default
                )
        else:
            st.application_messages_list_mutex.release()
            time.sleep(0.01)


# 🔥 IMPORTANT: only this printer thread; NO raw socket listener!
threading.Thread(target=app_printer, daemon=True).start()

# -----------------------------
# USER LOOP
# -----------------------------
while True:
    try:
        user = input().split()
    except EOFError:
        break

    if not user:
        continue

    cmd = user[0].lower()

    # JOIN <group>
    if cmd == "join" and len(user) >= 2:
        group_id = user[1]
        gsock = grp_join(group_id, DISCOVERY_IP, DISCOVERY_PORT, myid)
        if gsock is None:
            print(Color.F_Red + "JOIN failed." + Color.F_Default)
            continue

        list_of_groups_mutex.acquire()
        list_of_groups.append([gsock, group_id])
        list_of_groups_mutex.release()

    # SEND <group> <msg...>
    elif cmd == "send" and len(user) >= 3:
        group_id = user[1]
        message  = " ".join(user[2:])

        list_of_groups_mutex.acquire()
        target_sock = None
        for gsock, gname in list_of_groups:
            if gname == group_id:
                target_sock = gsock
                break
        list_of_groups_mutex.release()

        if target_sock is None:
            print(
                Color.F_Red
                + f"You are not a member of group {group_id}."
                + Color.F_Default
            )
        else:
            grp_send(target_sock, message, 0)

    # LEAVE <group>
    elif cmd == "leave" and len(user) >= 2:
        group_id = user[1]
        grp_leave(group_id, myid)

    # EXIT / QUIT
    elif cmd in ["exit", "quit"]:
        print(Color.F_LightYellow + "Exiting client..." + Color.F_Default)
        break

    else:
        print(Color.F_Red + "Invalid command." + Color.F_Default)
