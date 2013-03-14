# Mitch Andrews
# 10/01/10

import sys
import os
import subprocess
import re
import getopt
from ccfroutines import *
from crepositoryserver import *
#from cglobals import *
from ccold import *


#--------------------------------------------------------------
#--- Main -----------------------------------------------------

#output = ShellOutputLines(cmd + " " + path)

#print "---------"
#for i in range(0, len(output)):
#	print "output[%d]: %s/%s" % (i, path, output[i])

CClient = Cold()

# Getopt complete argument list
optlist, args = getopt.gnu_getopt(sys.argv[1:], 'a:c:df:hlo:pr:s:vw:',
				['pretend', 'verbose', 'debug', 'help', 'warranty',
				 'create-map', 'create-atlas', 'list-files', 'list-maps', 'print-usage', 'show-usage',

				 'find-file=', 'list-map-files=', 'map-regex=', 'options-file=',
				 'outpath=' 'receive=', 'scan-subnet=', 'send-file=', 'send-to-cloud=', 'update-piece='])

for i in optlist:
	flag, val = i
	if flag == "-d" or flag == "--debug":
		CClient.DebugOutput = True
		print "flag: -d"

	elif flag == "-v" or flag == "--verbose":
		CClient.VerboseOutput = True
		if CClient.DebugOutput == True:
			print "flag: -v"

	elif flag == "-p" or flag == "--pretend":
		CClient.PretendMode = True
		if CClient.DebugOutput == True:
			print "flag: -p"

	elif flag == "-h" or flag == "--help":
		if CClient.DebugOutput == True:
			print "flag: -h"
		#PrintProgramHelp()
		print "-INSERT PROGRAM HELP HERE-"
		sys.exit()

	elif flag == "--warranty":
		if CClient.DebugOutput == True:
			print "flag: --warranty"
		#PrintProgramWarranty()
		print "-INSERT PROGRAM WARRANTY HERE-"
		sys.exit()

	elif flag == "--list-files":
		if CClient.DebugOutput == True:
			print "flag: --list-files"
		files = CClient.ListFiles()
#		print "Files:",
		for f in files:
			print f

		sys.exit()

	elif flag == "--list-maps":
		if CClient.DebugOutput == True:
			print "flag: --list-maps"
		maps = CClient.ListMaps()
#		print "Maps:",
		for m in maps:
			print m

		sys.exit()

	elif flag == "--list-map-files":
		if CClient.DebugOutput == True:
			print "flag: --list-map-files %s" % val
		contents = CClient.ListMapFiles(val)
		print "Map file contents:"
		for c in contents:
			print c
		sys.exit()

	elif flag == "--map-regex":
		if val is not None:
			if CClient.DebugOutput == True:
				print "flag: --map-regex %s" % val
				CClient.SetFindFileMapRegex(val)
		else:
			print "Syntax Error: argument\"--max-regex\" requires an argument"
			sys.exit()

	elif flag == "-f" or flag == "--options-file":
		if CClient.DebugOutput == True:
			print "flag: -f %s" % val
		CClient.SetOptionsFile(val)

	elif flag == "-o" or flag == "--outpath":
		CClient.SetOutpath(val)
		if CClient.DebugOutput == True:
			print "flag: -o %s" % val



CClient.LoadOptions()

if CClient.VerboseOutput == True:
#	print "Module working dir:", ModuleWDir
#	print "Module absolute path:", ModulePathAbs
	print "Cache path:", CClient.CachePath
	
# list of Integers (KBs)
FreeSpace = []

# iterate through servers, append `df` results of each
for s in CClient.ServerList:
	# todo code not optimized
	if CClient.DebugOutput == True:
		print "Calling Df(): " + s.get_host()

	usage = s.Df()
	if usage > 0:
		if CClient.DebugOutput == True:
			print "appending (FreeSpace): " + str(usage)
		FreeSpace.append(usage)
	else:
		if CClient.DebugOutput == True:
			print "appending (ErrorServers): " + s.get_host()
		ErrorServers.append(s.get_host())

if CClient.VerboseOutput == True:
	#print hashspace information
	hashLowerBound = 0
	for s in CClient.ServerList:
		spacePercentage = float(float(FreeSpace[CClient.ServerList.index(s)]) / float(sum(FreeSpace)))
		hashUpperBound = int(int("ffffffffffffffffffffffffffffffffffffffff", 16) * float(float(FreeSpace[CClient.ServerList.index(s)]) / float(sum(FreeSpace)))) + hashLowerBound - 1
		if hashUpperBound >= 0x10000000000000000000000000000000000000000:
			hashUpperBound = 0xffffffffffffffffffffffffffffffffffffffff
		s.SetHashSpaceLowerBound(hashLowerBound)
		s.SetHashSpaceUpperBound(hashUpperBound)
		
		print "hashspace percentage: %.4f\nLowerBound: %x\nUpperBound: %x\n" % (spacePercentage, hashLowerBound, hashUpperBound)
		
		hashLowerBound = hashUpperBound + 1

# Re-scan the args list for action options

for i in optlist:
	flag, val = i

	if flag == "-l":
#		ListServerContents(ServerList)
		if CClient.DebugOutput == True:
			print "flag: -l"

	# -c, --create-map: Create Map
	elif flag == "-c" or flag == "--create-map" or flag == "--create-atlas":
		if CClient.DebugOutput == True:
			print "flag: -c %s" % val
		if len(val) != 0:
			ret = CClient.CreateMap(val)
		else:
			print "ERROR: -c <file>, len(file) == 0"
			sys.exit()

	# --find-file: Search maps for matching filenames
	elif flag == "--find-file":
		if CClient.DebugOutput == True:
			print "flag: --find-file %s" % val
		if len(val) != 0:
			ret = CClient.FindFile(val)

			#debug
			for r in ret:
				print "returned: " + r
		else:
			print "ERROR: --find-file <filename_regex>, len(filename_regex) == 0"
			sys.exit()

	# -r, --receive: Get File
	elif flag == "-r" or flag == "--receive":
		if CClient.DebugOutput == True:
			print "flag: -r %s" % val
		if len(val) > 0:
			ret = CClient.ReceiveFromCloud(val)
			if len(ret) > 0:
				print "Error receiving the following files:"
				for f in ret:
					print " " + f
		else:
			print "SYNTAX ERROR: -r <path>, len(path) == 0"

	# --scan-subnet
	elif flag == "-a" or flag == "--scan-subnet":
		if CClient.DebugOutput == True:
			print "flag: --scan-subnet %s" % val
		if len(val) != 0:
			for r in ListSubnetIPs(val):
				alive = IsHostAlive(r)
				if alive == True:
					print "host " + r + ": up"
				else:
					print "host " + r + ": down"
		else:
			print "ERROR: --scan-subnet <subnet>, len(subnet) == 0"

	# -s, --send-to-cloud: Send File To Cloud
	elif flag == "-s" or flag == "--send-to-cloud":
		if CClient.DebugOutput == True:
			print "flag: -s %s" % val
		if len(val) != 0:
			ret = CClient.SendToCloud(val)
		else:
			print "ERROR: -s <path>, len(path) == 0"

	# --show-usage
	elif flag == "--show-usage" or flag == "--print-usage":
		if CClient.DebugOutput == True:
			print "flag: ---show-usage"

		stats,err = CClient.GetUsageStatistics()

		if len(stats) > 0:
			print "available space in KB:"
			for i in stats:
				print i
		if len(err) > 0:
			for e in err:
				print "df server error: " + e

	# --update-piece: duplicate piece copies or delete copies across servers
	#					to meet redundancy specifications
	elif flag == "--update-piece":
		if CClient.DebugOutput == True:
			print "flag: --update-piece %s" % val
		if len(val) != 0:
			ret = CClient.UpdatePiece(val)
		else:
			print "ERROR: --update-piece <piecename>, len(piecename) == 0"

	# --update-map: duplicate piece copies or delete copies across servers
	#					to meet redundancy specifications
	elif flag == "--update-map":
		if CClient.DebugOutput == True:
			print "flag: --update-map %s" % val
		if len(val) != 0:
			downpieces, downfiles = CClient.UpdateMap(val)
			print "down pieces:",
			for p in downpieces:
				print p + '\n'
			print "down files:",
			for f in downfiles:
				print f + '\n'
		else:
			print "ERROR: --update-map <mappath>, len(mappath) == 0"




#for s in CClient.ServerList:
#	output1 = serverls(s.get_host(), s.get_user(), s.get_path())
#	print "output1: %s" % output1




#output2 = serverls('ppgbox', 'cold', '/home/cold/repository')
#print "output2: %s" % output2

#print ":", os.path.abspath('.')



#ORIGIFS="$IFS"
#IFS=$'\n'
#	LocalFileListTemp=( `ls -1 $RepositoryPath` )
#	LocalFileListTempSize=${#LocalFileListTemp[*]}
#IFS="$ORIGIFS"
#
## loop through initial entries and make them full-path rather than just filenames
#i=0
#while [[ $i -lt $LocalFileListTempSize ]]
#do
#	LocalFileListTemp[i]="$RepositoryPath/${LocalFileListTemp[i]}"
#	(( i++ ))
#done
