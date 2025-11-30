# middleware/threads.py
from threading import Thread
from .manager_listener import group_manager_listener
from .application_message_listener import application_message_listener
from .message_processor import application_message_processor
from .sender import application_sender
from .utils import debug_raw_sniffer   # <-- CORRECT IMPORT

def start_threads():
    threads = [
        ("group_manager_listener", group_manager_listener),
        ("application_message_listener", application_message_listener),
        ("application_message_processor", application_message_processor),
        ("application_sender", application_sender)
    ]

    for name, func in threads:
        t = Thread(target=func, name=name, daemon=True)
        t.start()
        print(f"[DEBUG] Thread {name} started ✔")

