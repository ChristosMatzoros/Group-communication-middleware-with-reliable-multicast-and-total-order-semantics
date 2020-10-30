#middleware program undertakes the whole communication between the members and to the group manager
import socket
import time
import pickle
import struct
import threading
import copy

MULTICAST_ADDRESS = "224.51.105.104"
first_join = 0
manager_address = 0		#Group manager information
tcp_socket = 0			#TCP socket
server_port = 0			#Server TCP port
message_list = []
LOSE_PACKETS_ENABLED = 0
LOSE_PACKETS_COUNTER = 0

#Locks for safe iterating over a list
mutex = threading.Lock()
states_list_mutex = threading.Lock()
application_messages_list_mutex = threading.Lock()
packets_list_mutex = threading.Lock()
send_list_mutex = threading.Lock()
id = ""
multicast_socket = 0
states_list = []
packets_list = []
application_messages_list = []
msgids_list = []	#used to clear msg buffer
send_list = []

#TCP socket creation
tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#Serialize data so that it can be transfered over the network
def serialize(data):
	result = pickle.dumps(data)
	return result

#Deserialize data received from the network
def deserialize(data):
	result = pickle.loads(data)
	return result

#Poll the group manager and receive and process messages sent by the group manager
def group_manager_listener():
	global id

	client_tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	while True:
		#Establish a connection to the group manager
		while True:
			try:
				client_tcp_socket.close()
				client_tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				client_tcp_socket.connect((manager_address[0], server_port))
			except:
				continue
			break

		#Send a poll message to the group manager
		msg = serialize(["POLL", id])
		client_tcp_socket.send(msg)

		#Receive a message from the group manager
		data = client_tcp_socket.recv(1024)
		while(data == b''):
			data = client_tcp_socket.recv(1024)
		data = deserialize(data)

		#If the message is "NACK", then there are no changes to the group formation
		if(data[0] == "NACK"):
			continue
		if(not data):
			continue

		#Append the received message to the messages list, so that it can be processed later on
		mutex.acquire()
		message_list.append(data)
		mutex.release()

		#Establish a connection to the group manager
		while True:
			try:
				client_tcp_socket.close()
				client_tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				client_tcp_socket.connect((manager_address[0], server_port))
			except:
				continue
			break

		#If the message received is a NEW_MEMBER message, then a new member is about to enter the group
		if(data[0] == "NEW_MEMBER"):
			group = data[1][0]
			myid = data[1][1]

			#Add the id of the new member to the local list of members for that particular group
			states_list_mutex.acquire()
			for state in states_list:
				if(state[0] == group):
					state[4].append(myid)
					cur_state = copy.copy(state)
			states_list_mutex.release()

			#Add a message to the application messages list so that the application can receive the update for the new member
			application_messages_list_mutex.acquire()
			application_messages_list.append(["GROUP","GROUP MESSAGE: "+data[1][1]+" has joined the group "+group+".",cur_state[1]])
			application_messages_list_mutex.release()

			#Send an acknowledgement to the group manager, letting it know that the update for the new member is received
			msg = serialize(["NEW_MEMBER_ACK"])
			client_tcp_socket.send(msg)
			continue

		#If the message received is a LEAVE_MEMBER message, then a member is about to leave the group
		if(data[0] == "LEAVE_MEMBER"):
			group = data[1][0]	#the name of the group that the member is about to leave
			myid = data[1][1]	#the id of the member that wants to leave
			cur_s = data[1][2]	#the sequence number of the member that wants to leave, in case that this member was the sequencer of its group

			#Remove the member from the local members group
			states_list_mutex.acquire()
			for state in states_list:
				if(state[0] == group):
					state[8] = cur_s
					state[4].remove(myid)
					cur_state = copy.copy(state)
			states_list_mutex.release()

			#Add a message to the application messages list so that the application can receive the update for the member
			application_messages_list_mutex.acquire()
			application_messages_list.append(["GROUP","GROUP MESSAGE: "+data[1][1]+" has left the group "+group+".",cur_state[1]])
			application_messages_list_mutex.release()

			#Send an acknowledgement to the group manager, letting it know that the update for the member leaving is received
			msg = serialize(["LEAVE_MEMBER_ACK"])
			client_tcp_socket.send(msg)
			continue

#Listen for application messages
def application_message_listener():
	global id
	global LOSE_PACKETS_COUNTER
	global LOSE_PACKETS_ENABLED
	while True:
		states_list_mutex.acquire()
		for state in states_list:
			multicast_socket = state[2]
			current_state = state
			states_list_mutex.release()
			multicast_socket.settimeout(0.2)	#Timeout listening for messages from the specific group

			#packet["TAG",[id,seqno],msg]
			try:
				packet,(addr,port) = multicast_socket.recvfrom(1024)
				if(LOSE_PACKETS_ENABLED == 1):
					LOSE_PACKETS_COUNTER += 1
					if(LOSE_PACKETS_COUNTER == 20):
						print("LOST PACKET")
						LOSE_PACKETS_COUNTER = 0
						states_list_mutex.acquire()
						continue
				#send ACK to achieve reliable network
				multicast_socket.sendto(serialize("ACK"),(addr,port))
			except socket.timeout as e:
				states_list_mutex.acquire()
				continue

			packet = deserialize(packet)
			packet.append(current_state)

			#Append the message to the packets list, so that it can be processed later on
			packets_list_mutex.acquire()
			packets_list.append(packet)
			packets_list_mutex.release()

			states_list_mutex.acquire()
		states_list_mutex.release()

#Process application messages
def application_message_processor():
	global id

	while True:
		#if there is a new packet that is received from the reliable network(that is stored in the packets_list_mutex list)
		#we obtain it and proceed to actions depending on the content of the packet
		packets_list_mutex.acquire()
		while(not packets_list):
			packets_list_mutex.release()
			packets_list_mutex.acquire()
		packets_list_mutex.release()

		#Pop a message from the packets list
		packets_list_mutex.acquire()
		packet = packets_list.pop(0)
		packets_list_mutex.release()

		msg = copy.copy(packet[2])
		pid_k = copy.copy(packet[1])	#pid_k = [pid,seqno] - unique id of the message
		group = copy.copy(packet[3][0])

		#find the correct state entry from the states_list that will be used later
		#based in the group name of the packet
		states_list_mutex.acquire()
		for state in states_list:
			if(state[0] == group):
				break
		states_list_mutex.release()
		states_list_mutex.acquire()

		#create a copy of the pid_k in order to avoid incorrect behaviour
		curr_pid = copy.copy(pid_k)

		#if the curr_pid list has 3 elements is of type SEQ message used to retrieve the sequence number
		#we remove this element from this list in order to achieve uniform attitude to the message proccessing
		if(len(curr_pid) == 3):
			curr_pid.remove("SEQ")

		#the following code is used in order to clean the msgids list and msgbuf for every client
		f=0
		for msgid in msgids_list:
			if(group == msgid[0] and curr_pid ==  msgid[1]):
				msgid[2]+=1
				if(msgid[2] == 2*len(state[4])):
					ms = copy.copy(msgid[1])
					ms2 = copy.copy(ms)
					if(ms in state[5]):
						try:
							state[5].remove(ms)
						except:
							f=1
							break
						if(len(state[4])!=1):
							ms2.append("SEQ")
							try:
								state[5].remove(ms2)
							except:
								f=1
								break
							for k in state[7]:
								if (k[0] == curr_pid):
									try:
										state[7].remove(k)
									except:
										f=1
										break
						f = 1
						break
		states_list_mutex.release()
		if(f==0):
			msgids_list.append([group,curr_pid,1])
		elif(f==1):
			continue

		states_list_mutex.acquire()

		found = 0
		#the following code ensures that we do not process a duplicate message
		#of one that already exists
		for pid in state[5]:
			if(pid==pid_k):
				found = 1
				break

		if (found == 0):
			#if the message is new we append the msgid to the list of msgids of the client for this specific group
			state[5].append(pid_k)
			my_state = copy.copy(state)
			states_list_mutex.release()
			if(pid_k[0] != id):
				#if this member is not the original sender of the specific message we
				#resend it to the multicast in order to achieve reliable multicast
				multicast_sender(my_state,pid_k,msg)

			if("SEQ" in msg[1]):
				msg[1].remove("SEQ")

			#if this member the sequencer for the specific group
			if(my_state[4][0] == id and msg[0] == "TRM-MSG"):
				states_list_mutex.acquire()
				#create a new sequence number
				state[8]+=1
				my_state = copy.copy(state)
				states_list_mutex.release()
				msg[1].append("SEQ")
				message = ["TRM-SEQ",msg[1],my_state[8]]
				#send the new sequence number for the specific message to the multicast
				multicast_sender(my_state,msg[1],message)

				#append the message to the application_messages_list in order to be
				#retrieved from the application
				application_messages_list_mutex.acquire()
				application_messages_list.append(["APP",msg[2],my_state[1]])
				application_messages_list_mutex.release()

			else:
				#if the current member is not the sequencer of the specific group.
				#depending on the type of the message(TRM-MSG or TRM-SEQ) we append different
				#tuples in the msgbuf
				found = 0
				states_list_mutex.acquire()
				for message in state[7]:
					states_list_mutex.release()
					if(message[0] == msg[1]):
						if(msg[0] == "TRM-MSG"):
							message[2] = msg[2]

						elif(msg[0] == "TRM-SEQ"):
							message[1] = msg[2]

						found = 1
						states_list_mutex.acquire()
						break
					states_list_mutex.acquire()
				states_list_mutex.release()
				if(found == 0):
					if(msg[0] == "TRM-MSG"):
						states_list_mutex.acquire()
						state[7].append([msg[1],-1,msg[2]])		#leave the sequence number of this message empty(-1) for the moment
						states_list_mutex.release()
					elif(msg[0] == "TRM-SEQ"):
						states_list_mutex.acquire()
						state[7].append([msg[1],msg[2],""])		#leave the content of the message empty("") for the moment
						states_list_mutex.release()

				while True:
					flag = 0
					states_list_mutex.acquire()
					for message in state[7]:
						if(message[1]!=-1 and message[2]!="" and message[1] == state[6]+1):
							state[7].remove(message)
							#increase the number of the messages that are ready(d)(and finally recieved with the correct order) from the application
							state[6]+=1
							cur_state = copy.copy(state)
							application_messages_list_mutex.acquire()
							application_messages_list.append(["APP",message[2],cur_state[1]])
							application_messages_list_mutex.release()
							flag = 1
							break
					states_list_mutex.release()
					if(flag == 0):
						break

		else:
			states_list_mutex.release()

#Send an application message to the group
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


#Thread declaration
group_manager_listener_thread = threading.Thread(name = "Group Manager Listener", target=group_manager_listener)
application_message_listener_thread = threading.Thread(name = "Application Message Listener", target=application_message_listener)
application_message_processor_thread = threading.Thread(name = "Application Message Processor", target=application_message_processor)
application_sender_thread = threading.Thread(name = "Application Sender Thread", target=application_sender)

group_manager_listener_thread.daemon = True
application_message_listener_thread.daemon = True
application_message_processor_thread.daemon = True
application_sender_thread.daemon = True


#Join a specific group
def grp_join(groupid, addr, port, myid):
	global first_join
	global manager_address
	global server_port
	global group_manager_listener_thread
	global id
	global multicast_socket

	#First time entering join from this client
	if(first_join==0):
		id = myid
		#UDP socket creation
		udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
		udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

		#Send a discover message using UDP to the group manager
		#in order to discover its ip address and port
		msg = serialize(["DISCOVER",[myid]])
		udp_socket.settimeout(1)

		while True:
			try:
				udp_socket.sendto(msg,(addr,port))
				#Get response from the group manager
				response, manager_address = udp_socket.recvfrom(1024)
			except socket.timeout as e:
				continue
			break

		response = deserialize(response)	#response=[tag, ACK/NACK]
		server_port = response[1][0]
		#If the message received from the group manager is DISCOVER_NACK, then another member has used the same id
		if(response[0] == "DISCOVER_NACK"):
			return(-1)

		#Start the threads
		application_message_listener_thread.start()
		application_message_processor_thread.start()
		application_sender_thread.start()

	tcp_socket_join = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	#Establish a connection to the group manager
	while True:
		try:
			tcp_socket_join.close()
			tcp_socket_join = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			tcp_socket_join.connect((manager_address[0], server_port))
		except:
			continue
		break

	#Send "JOIN" message to the group manager using TCP socket
	msg = serialize(["JOIN",[groupid, myid]])
	tcp_socket_join.send(msg)

	if(first_join==0):
		group_manager_listener_thread.start()
		first_join = 1

	leave = 0

	#Wait until receiving a "JOIN_ACK" message from the group manager
	while(True):
		mutex.acquire()
		for message in message_list:
			if(message[0] == "JOIN_ACK"):
				message_list.remove(message)
				leave = 1
				break
		mutex.release()
		if(leave==1):
			leave = 0
			break

	gsock = message[1][0]	#Group socket
	members_in_group = message[1][1]	#List containing the members of the group

	multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
	multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	multicast_socket.bind(('', int(gsock)))
	mreq = struct.pack("=4sl", socket.inet_aton("224.51.105.104"), socket.INADDR_ANY)
	multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

	#Append the information about this group to the local states list
	states_list_mutex.acquire()
	states_list.append([groupid,gsock,multicast_socket,0,members_in_group,[],0,[],0])
	states_list_mutex.release()

	return gsock

#Leave from the group
def grp_leave(groupid, myid):
	tcp_socket_leave = 0
	#Establish a connection to the group manager
	while True:
		try:
			tcp_socket_leave = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			tcp_socket_leave.connect((manager_address[0], server_port))
		except:
			tcp_socket_leave.close()
			tcp_socket_leave = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			continue
		break

	#Send "LEAVE" message to the group manager using TCP socket
	msg = ["LEAVE",[groupid, myid,-1]]

	states_list_mutex.acquire()
	for state in states_list:
		if(state[0] == groupid):
			if(myid == state[4][0]):
				msg[1][2] = state[8]
			states_list.remove(state)
	states_list_mutex.release()

	msg = serialize(msg)
	tcp_socket_leave.send(msg)

	#Wait until receiving a "JOIN_ACK" message from the group manager
	leave = 0
	while(True):
		mutex.acquire()
		for message in message_list:
			if(message[0] == "LEAVE_ACK"):
				message_list.remove(message)
				leave = 1
				break
		mutex.release()
		if(leave==1):
			leave = 0
			break
	tcp_socket_leave.close()

	#Remove the information regarding this group from the local states list
	states_list_mutex.acquire()
	for state in states_list:
		if(state[0] == groupid):
			states_list.remove(state)
	states_list_mutex.release()

#Send a message over UDP, including features such as acknowledgement receiving and resending
def multicast_sender(state,id_seqno,msg):
	global MULTICAST_ADDRESS
	global id

	packet = serialize(["RM-MSG",id_seqno,msg])

	while True:
		list_len = len(state[4])

		app_send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
		app_send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
		app_send_sock.settimeout(10)

		#Send the message using UDP
		app_send_sock.sendto(packet,(MULTICAST_ADDRESS,state[1]))

		start = time.time()
		#Receive acknowledgements from all the members of the group
		ack_counter=0
		while (ack_counter<list_len):
			try:
				ack_msg,(addr,port) = app_send_sock.recvfrom(1024)
			except socket.timeout as e:
				end = time.time()
				#If the time has passed and the acks are yet to be received, resend the message
				if(end - start > 5):
					break
				continue
			ack_msg = deserialize(ack_msg)
			if(ack_msg == "ACK"):
				ack_counter+=1

		app_send_sock.close()

		if(ack_counter<list_len):
			continue
		break

#send a new message to the group with a specific gsock
def grp_send(gsock,msg,length):
	global MULTICAST_ADDRESS
	global id	#the id of the sender

	states_list_mutex.acquire()
	for state in states_list:
		if(state[1] == gsock):
			state[3]+=1
			seqno = state[3]
			current_state = state
			states_list_mutex.release()
			msg = ["TRM-MSG",[id,seqno],msg]

			send_list_mutex.acquire()
			send_list.append([current_state,[id,seqno],msg])
			send_list_mutex.release()

			states_list_mutex.acquire()
			break
	states_list_mutex.release()


#Receive a message from a specific group
def grp_recv(gsock,type,message,len,block):
	found = 0
	while True:
		if(type == "APP"):
			application_messages_list_mutex.acquire()
			for msg in application_messages_list:
				#if the application has received a message with type == "APP"(application message)
				#and gsock == gsock of the message deliver the message to the application
				if((msg[0]=="APP") and (gsock == msg[2])):
					found = 1
					message[0]=msg[1]
					application_messages_list.remove(msg)
					break
			application_messages_list_mutex.release()
			if(found == 0):
				message[0]="NaN"
		elif(type == "GROUP"):
			application_messages_list_mutex.acquire()
			for msg in application_messages_list:
				#if the the application have received a message with type == "GROUP"(Group communication message)
				#and gsock == gsock of the message deliver the message to the application
				if((msg[0]=="GROUP") and (gsock == msg[2])):
					message[0]=msg[1]
					found = 1
					application_messages_list.remove(msg)
					break
			application_messages_list_mutex.release()
			if(found == 0):
				message[0]="NaN"
		if(block == 0 or found == 1):
			return
