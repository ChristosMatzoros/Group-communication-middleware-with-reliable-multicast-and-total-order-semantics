#Group manager program undertakes the whole management of the groups and for members actions that refer to group changes in the system
import socket
import struct
import pickle
import threading
import time
import sys
from Color import *
import signal

groups_list = []	#contains the groups as a list with tuples of this form:([groupid,multicast_port,[myid1,myid2,..]]).
members_list = []	#contains the members as a list with tuples of this form:([myid,TCP_ip_address, TCP_port_address,[]]).
message_list = []

stream_server_host = '127.0.0.1'
stream_client_port = int(sys.argv[1])	#The port of the group manager's side TCP connection. This port is send back to the client.

join_sem = threading.Semaphore(0)	#Used to block and unblock the join functionality(waits until everyone knows about the new join)
leave_sem = threading.Semaphore(0)	#Used to block and unblock the leave functionality(waits until everyone knows about the new join)

#these mutexes are used to achive synchronization and correct access of the list
message_list_mutex = threading.Lock()
members_list_mutex = threading.Lock()

multicast_port = 1030	#udp multicast port by default
gsock = 0

# Create UDP socket
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udp_socket.bind(('', 4242))
mreq = struct.pack("=4sl", socket.inet_aton("224.51.105.104"), socket.INADDR_ANY)
udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

#Functions serialize and deserialize are using pickle library to serialize the packet
#to binary and deserialize from binary to a specific data structure
def serialize(data):
	result = pickle.dumps(data)
	return result
def deserialize(data):
	result = pickle.loads(data)
	return result

#This handler is used to terminate the group manager thread correctly.
def signal_handler(sig, frame):
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
#i=0

#udp_listener listens to UDP multicast for messages
def udp_listener():
	while True:
		msg,(client_ip, client_port) = udp_socket.recvfrom(1024)
		msg = deserialize(msg)
		msg.append([client_ip,client_port])

		#retrieve information of the message
		content = msg[1]
		ip_address = msg[2][0]
		port_address = msg[2][1]

		id_exists=0
		myid = content[0]

		members_list_mutex.acquire()
		for member in members_list:
			#send discover NACK back to the client if there is already a member with the same id
			if member[0] == myid:
				msg = serialize(["DISCOVER_NACK",[stream_client_port]])
				udp_socket.sendto(msg,(ip_address, port_address))
				id_exists = 1
				break
		members_list_mutex.release()
		#send discover ACK back to the client if there is no member with the same id in the system
		if id_exists == 0:
			msg = serialize(["DISCOVER_ACK",[stream_client_port]])
			udp_socket.sendto(msg,(ip_address, port_address))

			#append the new member to the list of members
			members_list_mutex.acquire()
			members_list.append([myid,ip_address, port_address,[]])
			members_list_mutex.release()

#the function tcp_listener listens for messages to the specific TCP port either for
#poll messages or ACKs from all the other members in case of someone's join or leave
def tcp_listener():
	global join_sem
	global leave_sem
	# Create TCP socket
	tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	tcp_socket.bind(('', stream_client_port))
	tcp_socket.listen(1)

	while True:
		conn, (client_ip, client_port) = tcp_socket.accept()
		msg = conn.recv(1024)
		if(msg == b''):
			continue
		msg = deserialize(msg)

		msg2 = ["NACK", []]
		if(msg[0] == "POLL"):
			members_list_mutex.acquire()
			for member in members_list:
				if member[0] == msg[1]:
					if(not member[3]):	#if i have no new information for the specific member send a NACK
						msg2 = ["NACK", []]
					else:
						#if i have a new information for this member(NEW_MEMBER, LEAVE_MEMBER, JOIN_ACK, LEAVE ACK)
						#send it to the specific member
						msg2 = member[3].pop(0)
					break
			members_list_mutex.release()
			conn.send(serialize(msg2))
			conn.close()
			continue
		elif(msg[0] == "NEW_MEMBER_ACK"):
			#if the msg is NEW_MEMBER_ACK then release one time the join_sem(represents
			#that one member of the group have received the NEW_MEMBER msg)
			join_sem.release()
			conn.close()
			continue
		elif(msg[0] == "LEAVE_MEMBER_ACK"):
			#if the msg is LEAVE_MEMBER_ACK then release one time the leave_sem(represents
			#that one member of the group have received the LEAVE_MEMBER msg)
			leave_sem.release()
			conn.close()
			continue
		msg.append([client_ip,client_port])

		#append the message to list of messages so as to be processed
		message_list_mutex.acquire()
		message_list.append(msg)
		message_list_mutex.release()

		conn.close()


udp_thread = threading.Thread(name = "UDP Listener", target=udp_listener)
udp_thread.daemon = True
tcp_thread = threading.Thread(name = "TCP Listener", target=tcp_listener)
tcp_thread.daemon = True
udp_thread.start()
tcp_thread.start()

while True:
	#check for a new message in the list message_list
	message_list_mutex.acquire()
	while(not message_list):
		message_list_mutex.release()
		message_list_mutex.acquire()
	message_list_mutex.release()

	#if we find one pop it from the list
	message_list_mutex.acquire()
	element = message_list.pop(0)
	message_list_mutex.release()

	#element = [tag,[content],[ip_address, port_address]]
	tag = element[0]
	content = element[1]
	ip_address = element[2][0]
	port_address = element[2][1]

	members_in_group = []

	#if the tag is "JOIN", we must inform all the other members of the group for
	#the join of the new member. If it is the first member of the group we should address it.
	#At last we must for the acks "NEW_MEMBER_ACK" from all the other members of the groups
	if(tag == "JOIN"):
		groupid = content[0]
		myid = content[1]

		group_exists=0
		sem_counter = 0

		for group in groups_list:
			if group[0] == groupid:
				gsock = group[1]
				for id in group[2]:
					members_list_mutex.acquire()
					for member in members_list:
						if member[0] == id:
							member[3].append(["NEW_MEMBER",[groupid, myid]]) #append the "NEW_MEMBER" msg to members list for the specific member
							sem_counter+=1		#the counter is used for a general purpose semaphore
							break
					members_list_mutex.release()

				#this is used in order to make sure that we take acknowledgments
				#from all the members of the group for the join of this new member
				for i in range(0,sem_counter):
					join_sem.acquire()
				group[2].append(myid)
				members_in_group = group[2]
				group_exists = 1
				break
		#if the group does not exists create a new gsock and append information for the new group in the groups_list
		if group_exists == 0:
			multicast_port+=1
			gsock = multicast_port
			groups_list.append([groupid,multicast_port,[myid]])
			members_in_group = [myid]
		members_list_mutex.acquire()
		for member in members_list:
			if member[0] == myid:
				member[3].append(["JOIN_ACK", [gsock, members_in_group]])	# #append the "NEW_MEMBER" msg to members list for the specific member
				print(myid+" has joined group "+groupid+".")
				break
		members_list_mutex.release()

	elif(tag == "LEAVE"):
		groupid = content[0]
		myid = content[1]
		s = content[2]

		group_exists=0
		sem_counter = 0
		for group in groups_list:
			if group[0] == groupid:
				group[2].remove(myid)
				#if this member is the last of the group then remove the group from the groups_list
				if(not group[2]):
					groups_list.remove(group)
					break
				for id in group[2]:
					members_list_mutex.acquire()
					for member in members_list:
						if member[0] == id:
							member[3].append(["LEAVE_MEMBER",[groupid, myid,s]]) #append the "LEAVE_MEMBER" msg to members list for the specific member
							sem_counter+=1	#the counter is used for a general purpose semaphore
							break
					members_list_mutex.release()

				#this is used in order to make sure that we take acknowledgments
				#from all the members of the group for the leave of this member
				for i in range(0,sem_counter):
					leave_sem.acquire()
				break

		members_list_mutex.acquire()
		for member in members_list:
			if member[0] == myid:
				l = ["LEAVE_ACK"]
				member[3].append(l)	#append the "LEAVE_ACK" msg to members list for the specific member
				print(myid+" has left group "+groupid+".")
				break
		members_list_mutex.release()
