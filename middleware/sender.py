from .state import send_list, send_list_mutex
from .reliability import multicast_sender

def application_sender():
	while True:
		send_list_mutex.acquire()
		while(not send_list):
			send_list_mutex.release()
			send_list_mutex.acquire()
		send_list_mutex.release()

		#Pop a message from the send list
		send_list_mutex.acquire()
		packet = send_list.pop(0)
		send_list_mutex.release()

		#Send the message using the reliable network
		multicast_sender(packet[0],packet[1],packet[2])

