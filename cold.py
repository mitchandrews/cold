#!/usr/bin/python
#
# Mitch Andrews

import sys
import os
import subprocess
import re
import getopt
import random
import socket
import sqlite3
import time

from ccfroutines import *
from crepositoryserver import *
#from cglobals import *
from ccold import *

DebugOutput = False


## Assert daemon is running, get info to connect (properly) ##



## Instantiate socket to connect to daemon ##
sock_buf_size = 1024
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('localhost', 996))


# Getopt complete argument list
optlist, args = getopt.gnu_getopt(sys.argv[1:], 'a:c:df:hl:o:pqr:s:t:vw:',
				['exit', 'pretend', 'verbose', 'debug', 'help', 'warranty',
				 'consolidate-layout', 'print-usage', 'quit', 'show-usage',

				 'add-server=', 'find-file=', '--list=', 'list-files=',
				 'receive=', 'scan-subnet=', 'send-file=', 'send-to-cloud=', 'set-redundancy='])

for i in optlist:
	flag, val = i
	if flag == "-d" or flag == "--debug":
		DebugOutput = True

	elif flag == "-v" or flag == "--verbose":
		CClient.VerboseOutput = True
		if 	DebugOutput == True:
			print "flag: -v"

	elif flag == "-p" or flag == "--pretend":
		CClient.PretendMode = True
		if 	DebugOutput == True:
			print "flag: -p"

	elif flag == "-h" or flag == "--help":
		if 	DebugOutput == True:
			print "flag: -h"
		#PrintProgramHelp()
		print "-INSERT PROGRAM HELP HERE-"
		sock.close()
		sys.exit()

	elif flag == "--warranty":
		if 	DebugOutput == True:
			print "flag: --warranty"
		#PrintProgramWarranty()
		print "-INSERT PROGRAM WARRANTY HERE-"
		sock.close()
		sys.exit()


# Re-scan the args list for action options

for i in optlist:
	flag, val = i

	## --list-files: print files in db
	if flag == "-l" or flag == "--list-files" or flag == "--list":
		if 	DebugOutput == True:
			print "flag: --list-files", val
		#files = CClient.ListFiles()
		
		sock.send("l "+val.strip())
		fileListString = sock.recv(sock_buf_size).strip()
		#print " ## '--list-files' response:", fileListString
		
		print fileListString
		
		sock.close()
		sys.exit()
			
	# --add-server: Add unassigned server to list
	if flag == "--add-server":
		if len(val) != 0:
			if 	DebugOutput == True:
				print "flag: --add-server", val
				
			# truncate to fit packet buffer
			val = val.strip()
			s = (val[:(sock_buf_size-2)]) if len(val) > (sock_buf_size-2) else val

			print " # '--add-server' Sending: '" + "a "+s + "'"
			sent = sock.send("a "+s)
			data = sock.recv(sock_buf_size)
			print " ## '--add-server' response:", data.strip()
			
			sock.close()
			sys.exit()
		else:
			print "ERROR: --add-server <user@host:path>"
			sock.close()
			sys.exit()
			
	# --consolidate-layout: Reevaluate servers and redistribute pieces
	elif flag == "--consolidate-layout":
		if 	DebugOutput == True:
			print "flag: --consolidate-layout"

		sent = sock.send('c')
		data = sock.recv(sock_buf_size)
		print " ## '--consolidate-layout' response:", data.strip()
		
		sock.close()
		sys.exit()


	# --find-file: Search maps for matching filenames
	elif flag == "--find-file":
		if 	DebugOutput == True:
			print "flag: --find-file %s" % val
		if len(val) != 0:
			ret = CClient.FindFile(val)

			#debug
			for r in ret:
				print "returned: " + r
		else:
			print "ERROR: --find-file <filename_regex>, len(filename_regex) == 0"
			sock.close()
			sys.exit()

	# -r, --receive: Get File
	elif flag == "-r" or flag == "--receive":
		if 	DebugOutput == True:
			print "flag: -r %s" % val
		if len(val) > 0:
			
			dbpath = val
			localpath = ''
			
			for i in sys.argv[1:]:
				print "i:", i
				if i[0] != '-' and len(i) > 1:
					if i != dbpath:
						localpath = i
						break
						
			print "localpath:", localpath
			print "dbpath:", dbpath
			
			sock.send('g \''+localpath + '\' \'' + dbpath + '\'')
			data = sock.recv(sock_buf_size)
			print " ## '--receive' response:", data
			
		else:
			print "SYNTAX ERROR: -r '<dbpath>' '<localpath>'"

	# --scan-subnet
	elif flag == "-a" or flag == "--scan-subnet":
		if 	DebugOutput == True:
			print "flag: --scan-subnet %s" % val
		if len(val) != 0:
			for r in ListSubnetIPs(val):
				alive = IsHostAlive(r)
				if alive == True:
					print "host " + r + ": up"
					#TODO: CClient.AddServer()...
				else:
					print "host " + r + ": down"
		else:
			print "ERROR: --scan-subnet <subnet>, len(subnet) == 0"

	# -s, --send-to-cloud: Send File To Cloud
	elif flag == "-s" or flag == "--send-to-cloud":
		if 	DebugOutput == True:
			print "flag: -s %s" % val
			
		if len(val) > 0:
			
			localpath = val
			dbpath = '/'
			
			for i in sys.argv[1:]:
				print "i:", i
				if i[0] != '-' and len(i) > 1:
					if i != localpath:
						dbpath = i
						break
						
			print "localpath:", localpath
			print "dbpath:", dbpath
			
			sock.send('p \''+localpath + '\' \'' + dbpath + '\'')
			data = sock.recv(sock_buf_size)
			print " ## '--send-to-cloud' response:", data
	
		else:
			print "ERROR: -s '<localpath>' '<dbpath>'  (quotes required!)"
			
	# --set-redundancy
	elif flag == "--set-redundancy":
		if 	DebugOutput == True:
			print "flag: --set-redundancy %s" % val
		if len(val) != 0 and int(val) > 0:
		
			#CClient.ChangeRedundancy(int(val))
			
			sock.send('r '+val)
			data = sock.recv(sock_buf_size)
			print " ## '--set-redundancy' response:", data
			
		else:
			print "ERROR: --set-redundancy <val>; val must be > 1"
			
		sock.close()
		sys.exit()

	# --show-usage
	elif flag == "--show-usage" or flag == "--print-usage":
		if 	DebugOutput == True:
			print "flag: ---show-usage"

		sock.send('u')
		data = sock.recv(sock_buf_size)
		print " ## '--show-usage' response:", data

		# if len(stats) > 0:
			# print "available space in KB:"
			# for i in stats:
				# print i
		# if len(err) > 0:
			# for e in err:
				# print "df server error: " + e
				
		sock.close()
		sys.exit()
		
	# quit
	elif flag == "-q" or flag == "--quit" or flag == "--exit":
		if 	DebugOutput == True:
			print "flag: --quit"

		sent = sock.send('q')
		data = sock.recv(sock_buf_size)
		print " ## '--quit' response:", data.strip()
		
		sock.close()
		sys.exit()
		
sock.close()
sys.exit()