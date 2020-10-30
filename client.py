#The client program simulates a chat application

import middleware
import sys
import time
import socket
import time
import threading
from Color import *

#mutex used to achive synchronization and correct accesses of list_of_groups list
#between the listener_thread and the main thread
list_of_groups_mutex = threading.Lock()
list_of_groups = []

#COMMANDS information
commands_info = """
*** Commands Information ***
JOIN	:join "group name"
SEND	:send "group_name" "message"
LEAVE	:leave "group_name"
EXIT	:quit (or exit)
"""
print(Color.F_LightYellow, commands_info, Color.F_Default)


myid = sys.argv[1]
addr = "224.51.105.104"
port = 4242

#the listener() function is used to receive messages both for group manager and from other members
def listener():
	while True:
		rec_msg = [""]
		list_of_groups_mutex.acquire()
		for group in list_of_groups:
			list_of_groups_mutex.release()
			middleware.grp_recv(group[0],"GROUP",rec_msg,1024,0)	#check for a new "GROUP" message that pertain to group management changes
			if(rec_msg[0] != "NaN"):
				print(Color.F_Red, rec_msg[0], Color.F_Default)
			middleware.grp_recv(group[0],"APP",rec_msg,1024,0)		#check for a new "APP" message that pertain to message from another member
			if(rec_msg[0] != "NaN"):
				print(Color.F_Green, group[1]+": "+rec_msg[0], Color.F_Default)
			list_of_groups_mutex.acquire()
		list_of_groups_mutex.release()

listener_thread = threading.Thread(name = "Listener Thread", target=listener)
listener_thread.daemon = True	#used in order to achieve correct termination of the thread
listener_thread.start()

while True:
	data = input("")			#read input from the user
	input_array = data.split() 	#use split to retrieve information of the command
	if(input_array[0] == "join"):
		group_name = input_array[1]
		gsock = middleware.grp_join(group_name, addr, port, myid)
		#if gsock==-1, it means that there is another member with the same id in the system
		#so we need to reject it
		if(gsock == -1):
			print("This myid is already taken by another user. Please choose a different one.")
			sys.exit(1)
		print("The group socket for the group \""+ group_name+"\" is "+str(gsock)+".")

		#append that the new group in the list_of_groups in order to consider
		#new messages for this members that concerns this group
		list_of_groups_mutex.acquire()
		list_of_groups.append([gsock,group_name])
		list_of_groups_mutex.release()
	elif(input_array[0] == "leave"):
		group_name = input_array[1]
		middleware.grp_leave(group_name, myid)
		list_of_groups_mutex.acquire()
		for group in list_of_groups:
			if(group[1] == group_name):
				list_of_groups.remove(group)	#remove this specific group from the list_of_groups
				break
		list_of_groups_mutex.release()

	elif(input_array[0] == "send"):
		group_name = input_array[1]
		#pop the first 2 elements of the list("send" and "group_name") in order for
		#the input to contain the message that we must send
		input_array.pop(0)
		input_array.pop(0)
		msg = " ".join(input_array)
		list_of_groups_mutex.acquire()

		#search for the correct gsock number from the group name that is given from the user
		for group in list_of_groups:
			if(group[1] == group_name):
				gsock = group[0]
				break
		list_of_groups_mutex.release()

		middleware.grp_send(gsock,msg,0)
	elif(input_array[0] == "quit" or input_array[0] == "exit"):	#exit the program
		break
	else:
		print("Incorrect command. Please try again.")

sys.exit(1)
