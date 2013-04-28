#!/usr/bin/python
#
# Mitch Andrews
#
# Cold Daemon
#
# coldd.py program dependencies:
#  global imports (sys, os, et al)
#

from crepositoryserver import *

import getopt
import sys
import os
import subprocess
import re
import hashlib
import paramiko
import random
import tempfile
import shutil
import signal
import socket
import string
import sqlite3
import threading
import time

from ccfroutines import *
from cglobals import *
from SQLiteDataSource import *
from ccold import *

client = 0
address = 0
def CleanExit(a, b):
	print " ## CleanExit()", client
	#os.remove("/tmp/.cold_pid")
	if client != 0:
		client.close()
	sys.exit(0)

	
VerboseOutput = False
DebugOutput = False
OptionsF = ''


## Assert user == root ##


# Getopt argument list
optlist, args = getopt.gnu_getopt(sys.argv[1:], 'vdhwo:',
				['verbose', 'debug', 'help', 'warranty',
				 'options-file='])
				 
for i in optlist:
	flag, val = i
	if flag == "-d" or flag == "--debug":
		DebugOutput = True
		VerboseOutput = True
	elif flag == "-v" or flag == "--verbose":
		VerboseOutput = True
	elif flag == "-h" or flag == "--help":
		print "help me!!"
		CleanExit()
	elif flag == "-w" or flag == "--warranty":
		print "help me!!"
		CleanExit()
	elif flag == "-o" or flag == "--options-file":
		print "Options file:", val
		OptionsF = val
		
		

## Init
random.seed()

CClient = Cold()

if len(OptionsF) > 0:
	CClient.OptionsF = OptionsF
CClient.LoadOptions()

CClient.VerboseOutput = VerboseOutput
CClient.DebugOutput = DebugOutput

for s in CClient.ServerList:
	#s.StartSSH()
	print ' ## SSHConn ' + s.get_host(), s.SSHCon


## Catch signals to do a clean exit
#signal.signal(signal.CTRL_C_EVENT, CleanExit)
#signal.signal(signal.CTRL_BREAK_EVENT, CleanExit)
signal.signal(signal.SIGQUIT, CleanExit)
signal.signal(signal.SIGINT, CleanExit)
signal.signal(signal.SIGABRT, CleanExit)


## Spawn listening socket
suck_buf_size = 1024
serversocket = 0

sockConnected = False
while not sockConnected:
	try:
		serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		serversocket.bind(('localhost', 996))
	except Exception:
		print 'socket.error: [Errno 98] Address already in use; sleeping 15 seconds'
		time.sleep(15)
		continue
	
	sockConnected = True

serversocket.listen(5)

print " ## Socket initialized"

while 1:
	# accept connections
	print " ## Socket listening"
	
	(client, address) = serversocket.accept()
	data = client.recv(suck_buf_size).strip()
	if data:
		if data == "c": #"--consolidate-layout":
			print " ## Socket received: --consolidate-layout"
			CClient.ConsolidateLayout(False)
			client.send("!")
			#client.send("Consolidate Complete!")
			
		elif data[0] == 'a': #"--add-server <server>":
			print " ## Socket received: --add-server", data
			CClient.AddServer(data[2:])
			client.send("!")
			
		elif flag == 'u':
			if 	DebugOutput == True:
				print " ## Socket received: --show-usage"

			(stats, err) = CClient.GetUsageStatistics()
			client.send("!")
			
		elif data[0] == 'r': #"--set-redundancy <val>":
			print " ## Socket received: --set-redundancy", data
			CClient.ChangeRedundancy(data[2:])
			client.send("!")
			
		elif data[0] == 'p': #"--send-to-cloud":
			print " ## Socket received: --send-to-cloud", data
			
			parts = string.split(data, '\'')
			localpath = parts[1]
			dbpath = parts[3]
			
			print " ## Socket parsed localpath:", localpath
			print " ## Socket parsed dbpath:", dbpath
			
			CClient.SendToCloud(localpath, dbpath)
			client.send("!")
			
		elif data[0] == 'g': #"--receive":
			print " ## Socket received: --receive", data
			
			parts = string.split(data, '\'')
			localpath = parts[1]
			dbpath = parts[3]
			
			print " ## Socket parsed localpath:", localpath
			print " ## Socket parsed dbpath:", dbpath
			
			CClient.ReceiveFromCloud(dbpath, localpath)
			client.send("!")
			
		elif data[0] == 'l': #"--list-files <dbpath>":
			print " ## Socket received: --list-files", data
			
			dbpath = '/'
			if len(data) > 2:
				dbpath = data[2:]
						
			fList = CClient.ListFiles(dbpath)

			listString = ""
			for f in fList:
				listString = listString + f + '\n'
				
			print " ## Socket liststring := ", listString
			client.send(listString)
			
			
		elif data[0] == 'q': #"--quit":
			print " ## Socket received: quit", data
			client.send("!")
			client.close()
			CleanExit()
			
	client.close()

CleanExit()
