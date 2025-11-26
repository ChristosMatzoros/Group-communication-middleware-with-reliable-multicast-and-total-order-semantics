from .state import (
    states_list, packets_list, packets_list_mutex,
    application_messages_list, application_messages_list_mutex,
    msgids_list, id, states_list_mutex
)
from .reliability import multicast_sender
from .transport import serialize, deserialize
import copy

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