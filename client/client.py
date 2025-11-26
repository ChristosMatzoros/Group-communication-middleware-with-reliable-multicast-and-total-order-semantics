import sys
import threading
from middleware.api import grp_join, grp_leave, grp_send, grp_recv
from shared.color import Color

# -----------------------------
# CONFIG
# -----------------------------
myid = sys.argv[1]

DISCOVERY_IP   = "224.51.105.104"    # 🔥 MATCHES api.py
DISCOVERY_PORT = 4242

list_of_groups = []
list_of_groups_mutex = threading.Lock()

print(Color.F_LightYellow + "*** Commands ***\nJOIN <group>\nSEND <group> <msg>\nLEAVE <group>\nEXIT\n" + Color.F_Default)

# -----------------------------
# LISTENER THREAD
# -----------------------------
def listener():
    while True:
        rec = [""]

        list_of_groups_mutex.acquire()
        for gsock, group in list_of_groups:
            list_of_groups_mutex.release()

            grp_recv(gsock, "APP", rec, 1024, 0)
            if rec[0] != "NaN":
                print(Color.F_Green + f"[{group}] {rec[0]}" + Color.F_Default)

            list_of_groups_mutex.acquire()
        list_of_groups_mutex.release()

threading.Thread(target=listener, daemon=True).start()

# -----------------------------
# USER LOOP
# -----------------------------
while True:
    user = input().split()
    if not user:
        continue

    cmd = user[0].lower()

    if cmd == "join" and len(user) >= 2:
        group_id = user[1]
        gsock = grp_join(group_id, DISCOVERY_IP, DISCOVERY_PORT, myid)  # 🔥 FIXED HERE
        list_of_groups_mutex.acquire()
        list_of_groups.append([gsock, group_id])
        list_of_groups_mutex.release()

    elif cmd == "send" and len(user) >= 3:
        group_id = user[1]
        message  = " ".join(user[2:])
        list_of_groups_mutex.acquire()
        for gsock, gname in list_of_groups:
            if gname == group_id:
                grp_send(gsock, message, 0)
        list_of_groups_mutex.release()

    elif cmd == "leave" and len(user) >= 2:
        grp_leave(user[1], myid)

    elif cmd in ["exit", "quit"]:
        print(Color.F_LightYellow + "Exiting client..." + Color.F_Default)
        break

    else:
        print(Color.F_Red + "Invalid command." + Color.F_Default)
