# Mitch Andrews
# 11/15/10

# Cold class
#
# ccold.py program dependencies:
#  global imports (sys, os, et al)
#

from crepositoryserver import *

import sys
import os
import subprocess
import re
import hashlib
import random
import tempfile

from ccfroutines import *
from cglobals import *

class Callable:
	def __init__(self, anycallable):
		self.__call__ = anycallable



# This holds the results of a LookupFile() or LookupPiece() call.
# Data only, no methods
class LookupResultStruct:
	def __init__(self):
		# names of pieces not 100%  available
		self.unavailablepieces = []
		# names of pieces 100%  available
		self.availablepieces = []

		# names of files not 100%  available
		self.unavailablefiles = []
		# names of files 100%  available
		self.availablefiles = []

		# total # of pieces
		self.piececount = -1

		# total # of files
		self.filecount = -1

class Cold:

	def __init__(self):
		self.OptionsF = "options.txt"
		self.LogF = "log.txt"
		self.RepositoryPath = "./repository"
		self.CachePath = ModulePathAbs + "/.cache"
		self.Outpath = "."

		self.EnableServerCache = False

		self.PreserveEmptyFolders = True

		# servers with less than this available space will throw errors
		self.ServerFreeMinMB = 2048

		self.PieceSize = 2048
		self.PiecePath = "./pieces"
		self.MapPath = "./maps"
		
		# MaximumRedundancy only used with 'random'
		self.MinimumRedundancy = 1
		self.MaximumRedundancy = 5
		#self.RedundancyOrdering = "random"
		self.RedundancyOrdering = "usage-proportional"

		# For 'usage-proportional', ServerList lists redundant servers
		# together, for example, if there is 1 redundant servers among
		# 3 total, A, A, B, with each letter representing a data set,
		# they will be listed in ServerList in the order A, A, B. In
		# this case 'MaximumRedundancy' is not used, 'MinimumRedundancy'
		# defines the copy count.
		self.ServerList = []

		self.VerboseOutput = True
		self.DebugOutput = True
		self.PretendMode = False

		self.FindFileMapRegex = ''

		if not os.path.isdir(self.CachePath):
			os.mkdir(self.CachePath)


	# Get/Set the options file
	# return:
	#	self.OptionsF	if file argument is not supplied, otherwise:
	#	true	if successful
	#	false	if file read error
	def OptionsFile(self, file=''):
		if file == "":
			return self.OptionsF

		# Check existence, readability
		if not os.access(file, os.F_OK | os.R_OK):
			print "ERROR: reading options: " + path
			return False

		self.OptionsF = file
		return True

	# Get/Set the log file
	# return:
	#	self.LogF	if file argument is not supplied, otherwise:
	#	true	if successful
	#	false	if file writability error
	def LogFile(self, file):
		if file == "":
			return self.LogF

		# Check existence, readability
		if not os.access(file, os.W_OK):
			print "ERROR: writing log file: " + path
			return False

		self.LogF = file
		return True

	# Set the Output Path for receiving files
	# return:
	#	true	if successfully exists/created
	#	false	if still cannot access after creation attempt
	def SetOutpath(self, path):
		# If path already exists, assign & return:
		if os.path.isdir(path):
			self.Outpath = path
			if self.DebugOutput == False:
				print "Outpath := " + os.path.dirname(path)
			return True
		# If path doesn't exist, create
		else:
			if self.DebugOutput == False:
				print "SetOutpath making Outpath (0777): " + os.path.dirname(dirname(path))
			if not os.path.exists(path):
				os.makedirs(path)

		# After creating, assert it was actually created
		if os.path.isdir(path):
			self.Outpath = path
			if self.DebugOutput == False:
				print "Outpath := " + os.path.dirname(path)
			return True
		# If not, error and quit
		else:
			print "ERROR: cannot set Outpath: " + os.path.dirname(path)

		return False

	# Set the Map Regex for argument --find-file
	# Returns:
	#	void
	# Completeness: 80%
	def SetFindFileMapRegex(self, regex):
		if regex is not None and len(str(regex)) > 0:
			if self.DebugOutput == True:
				print "Setting FindFileMapRegex: " + str(regex)
			self.FindFileMapRegex = str(regex)

	# 1) Read user-defined options from file using global regexs,
	# 2) Populate global vars with file contents
	def LoadOptions(self):

		# do checks on the options file first
		OptionLines = ShellOutputLines('cat ' + self.OptionsF)
		for j in OptionLines:

			# Verbose Output
			r = re.match(r'^[ \t]*verbose[ \t]*=[ \t]*(.*)$', j)
			if r is not None:
				if r.group(1).lower() == "True":
					print "verbose output on"
					self.VerboseOutput = True
				continue

			# Debug Output
			r = re.match(r'^[ \t]*debug[ \t]*=[ \t]*(.*)$', j)
			if r is not None:
				if r.group(1).lower() == "True":
					print "debug output on"
					self.DebugOutput = True
					if self.VerboseOutput == False:
						print "verbose output on"
						self.VerboseOutput = True
				continue

			# Repository Path
			r = re.match(r'^[ \t]*repository_path[ \t]*=[ \t]*(.*)$', j)
			if r is not None:

				# todo repository path integrity checks here
				#  (or conglomorate environment sanity check after loading file)

				self.RepositoryPath = r.group(1)
				if self.DebugOutput == True:
					print "setting repository path: %s" % self.RepositoryPath
				continue

			# Cache path
			r = re.match(r'^[ \t]*cache[-]?path[ \t]*=[ \t]*(.*)$', j)
			if r is not None:

				# todo integrity checks here
				#  (or conglomorate environment sanity check after loading file)

				if not os.path.isdir(r.group(1)):
					os.mkdir(r.group(1))

				# if creating it doesn't create it, then we have an error
				if not os.path.isdir(r.group(1)):
					print "ERROR: setting CachePath: %s" % r.group(1)
					continue

				self.CachePath = r.group(1)
				if self.DebugOutput == True:
					print "setting CachePath: %s" % self.CachePath

				continue

			# Log File
			r = re.match(r'^[ \t]*logfile[ \t]*=[ \t]*(.*)$', j)
			if r is not None:

				# todo integrity checks here
				#  (or conglomorate environment sanity check after loading file)

				self.LogF = r.group(1)
				if self.DebugOutput == True:
					print "setting log file: %s" % self.LogF
				continue

			# Server List
			r = re.match(r'^[ \t]*add[-]?server[ \t]*(.*)$', j)
			if r is not None:

				# todo integrity checks here
				#  (or conglomorate environment sanity check after loading file)

				# regex to gather hostname
				r = re.split('[ \t@:]+', j)
				if len(r[1]) == 0:
					print "ERROR: host in: " + j
					sys.exit()
				if len(r[2]) == 0:
					print "ERROR: user in: " + j
					sys.exit()
				if len(r[3]) == 0:
					print "ERROR: path in: " + j
					sys.exit()

				rs = RepositoryServer()
				rs.set_user(r[1])
				rs.set_host(r[2])
				rs.set_path(r[3])

				# Verify passwordless access to the server
				if HasPasswordlessSSH(rs.get_host(), rs.get_user()):
	#				print "SSH is passwordless!"
					MuteSSHLogin(rs.get_host(), rs.get_user())
				else:
					print "Fatal Error: password required for SSH to %s@%s" % (rs.get_user(), rs.get_host())

				# Check free space in MB, only add server if minimum is met
				MBfree = rs.Df()/1024
				if MBfree < float(self.ServerFreeMinMB):
					print "WARNING: %s space low, not adding: %f MB < %d MB" % (rs.get_host(), MBfree, self.ServerFreeMinMB)
					continue
				self.ServerList.append(rs)
				if self.DebugOutput == True:
					rs.print_info()

				continue

			# Map path
			r = re.match(r'^[ \t]*map[-]?path[ \t]*=[ \t]*(.*)$', j)
			if r is not None:

				# todo integrity checks here
				#  (or conglomorate environment sanity check after loading file)
				#remove trailing '/' from path if it exists:
				self.MapPath = r.group(1)
				if self.MapPath[-1] == "/":

					print "deleting self.MapPath[-1:]"
					del self.MapPath[-1:]

				if self.DebugOutput == True:
					print "setting map path: %s" % self.MapPath
				continue

			# Map creation piece size
			r = re.match(r'^[ \t]*piece[-]?size[ \t]*=[ \t]*(.*)$', j)
			if r is not None:
				if r.group(1).lower() != "":
					# todo integrity checks here
					self.PieceSize = int(r.group(1))
					if self.DebugOutput == True:
						print "setting piece size: " + str(self.PieceSize)

				continue

			# Minimum Redundancy
			r = re.match(r'^[ \t]*minimum[-]?redundancy[ \t]*=[ \t]*(.*)$', j)
			if r is not None:
				if r.group(1).lower() != "":
					# todo integrity checks here
					self.MinimumRedundancy = int(r.group(1))
					if self.DebugOutput == True:
						print "setting minimum redundancy: " + str(self.MinimumRedundancy)

				continue

			# Maximum Redundancy
			r = re.match(r'^[ \t]*maximum[-]?redundancy[ \t]*=[ \t]*(.*)$', j)
			if r is not None:
				if r.group(1).lower() != "":
					# todo integrity checks here
					self.MaximumRedundancy = int(r.group(1))
					if self.DebugOutput == True:
						print "setting maximum redundancy: " + str(self.MaximumRedundancy)

				continue

			# Redundancy Ordering
			r = re.match(r'^[ \t]*redundancy[-]?ordering[ \t]*=[ \t]*(.*)$', j)
			if r is not None:
				if r.group(1).lower() != "":
					# todo more integrity checks here
					if r.group(1) == "random":
						self.RedundancyOrdering = "random"
						if self.DebugOutput == True:
							print "setting redundancy ordering: " + self.RedundancyOrdering
					elif r.group(1) == "fcfs":
						self.RedundancyOrdering = "fcfs"
						if self.DebugOutput == True:
							print "setting redundancy ordering: " + self.RedundancyOrdering

				continue

			# Minimum Server Free Space in MB
			r = re.match(r'^[ \t]*server[-]?free[-]?minimum[-]?mb[ \t]*=[ \t]*(.*)$', j)
			if r is not None:
				if r.group(1).lower() != "":
					# todo integrity checks here
					self.ServerFreeMinMB = int(r.group(1))
					if self.DebugOutput == True:
						print "setting server minimum free space MB: %d" % self.ServerFreeMinMB

				continue

			# Preserve Empty Folders
			r = re.match(r'^[ \t]*preserve-empty-folders[ \t]*=[ \t]*(.*)$', j)
			if r is not None:
				if r.group(1).lower() == "True":
					self.PreserveEmptyFolders = True
					if self.VerboseOutput == True:
						print "preserving empty folders"
				elif r.group(1).lower() == "False":
					self.PreserveEmptyFolders = False
					if self.VerboseOutput == True:
						print "not preserving empty folders"
				continue
			# set more option variables here

#	LoadOptions = Callable(LoadOptions)


	# PURPOSE: given a list of servers, return the list of servers that are in self.ServerList
	#			and NOT in the supplied list; i.e. return the complement of the list
	def GetComplementServerList(self, serverlist):

		# return variable
		complementlist = []

		# boolean flag whether or not to add
		found = False
		for t in self.ServerList:
			for s in serverlist:
				if s.get_host() == t.get_host():
					found = True
					#break
			if found == False:
				complementlist.append(t)
			found = False

		return complementlist


	# PURPOSE: return a list up to length n of available servers using the current ordering
	# for now it is assumed that server accessability is asserted when they are added,
	# so almost no sanity checks are required
	def GetNRedundancyServers(self, serverlist, n, redundancyordering = 'default'):
		randomizedservers = []
		serverlistcopy = []
		serverlistcopy.extend(serverlist)

 		if redundancyordering == 'default':
 			redundancyordering = self.RedundancyOrdering

 		if redundancyordering == "random":
 			for i in range(n):
 				if len(serverlistcopy) > 0:
 					rand = random.randrange(0, len(serverlistcopy))
 					randomizedservers.append(serverlistcopy[rand])
 					del serverlistcopy[rand]
					
		if redundancyordering == "usage-proportional":
			for i in range(n):
				if len(serverlistcopy) > 0:
					randomizedservers.append(serverlistcopy[0])
					serverlistcopy.pop(0)

# 		if self.DebugOutput == True:
 #			print "server order := ",
 #			i=0
 #			while i < len(randomizedservers):
 #				print randomizedservers[i].get_host() + ' ',
 #				i=i+1
 #			print '\n'

 		return randomizedservers
#
# 	GetNRedundancyServers = Callable(GetNRedundancyServers)


	# Purpose:
	#	Get disk usage information about a file server, for quotas, reporting, whatever
	# Called by "--show-usage" flag
	def GetUsageStatistics(self):

		# list of Integers (KBs)
		FreeSpace = []

		# list of Strings (hostnames)
		ErrorServers = []

		# iterate through servers, append `df` results of each
		for s in self.ServerList:
			# todo code not optimized
			if self.DebugOutput == True:
				print "Calling Df(): " + s.get_host()

			usage = s.Df()
			if usage > 0:
				if self.DebugOutput == True:
					print "appending (FreeSpace): " + str(usage)
				FreeSpace.append(usage)
			else:
				if self.DebugOutput == True:
					print "appending (ErrorServers): " + s.get_host()
				ErrorServers.append(s.get_host())
				
		# print hashspace information
		hashLowerBound = 0
		for s in self.ServerList:
			spacePercentage = float(float(FreeSpace[self.ServerList.index(s)]) / float(sum(FreeSpace)))
			hashUpperBound = int(int("ffffffffffffffffffffffffffffffffffffffff", 16) * float(float(FreeSpace[self.ServerList.index(s)]) / float(sum(FreeSpace)))) + hashLowerBound - 1
			s.SetHashSpaceLowerBound(hashLowerBound)
			s.SetHashSpaceUpperBound(hashUpperBound)
			
			print "hashspace percentage: %.4f\nLowerBound: %x\nUpperBound: %x\n" % (spacePercentage, hashLowerBound, hashUpperBound)
			
			hashLowerBound = hashUpperBound + 1

		return FreeSpace, ErrorServers


	# List the files in the map folder
	# Returns:
	#	list of filenames in self.MapPath (strings)
	def ListMaps(self):
		# return variable (list) containing the file names
		filenames = []
		if os.path.isdir(self.MapPath):
			#os.listdir(self.MapPath)
			maps = ShellOutputLines("ls \""+ self.MapPath + "\" | grep '.map$'")

			# strip() each map of whitespace and append
			i=0
			while i<len(maps):
				filenames.append(maps[i].strip())
				i=i+1

		return filenames

	# List the files contained in all the maps in the maps folder
	# Returns:
	#	list of file names (strings)
	def ListFiles(self):
		# return variable
		filelist = []

		# list all maps
		maps = self.ListMaps()

		# append files from each map to return variable
		for m in maps:
			filelist.extend(self.ListMapFiles(m))

		return filelist


	# Create the map and cache files for the supplied path,
	# returns a list of paths to the pieces
	def CreateMap(self, path, mapfileoption = ''):

		# return value
		piecelist = []

		mappath = ""

		# remove trailing slash(es)
		path = path.rstrip("/")

		if mapfileoption != "":
# and (os.path.isfile(mapfileoption) or os.path.isdir(mapfileoption)):
			mappath = mapfileoption
		else:
			mappath = self.MapPath + "/" + path + ".map"

		if self.DebugOutput == True:
			print "procesing: " + path

		# Check if it's a file
		if os.path.isfile(path):
			# Check readability
			if not os.access(path, os.R_OK):
				print "ERROR: can not read: " + path
				sys.exit()

			source = file(path, "rb")

			mapfile = file(mappath, "a")

			# print the path to the map file
			mapfile.write(path + '\n')

			buffer = source.read(self.PieceSize*1024)
			while len(buffer) != 0:
				# rename the file to its MD5
				#md = hashlib.md5(buffer).hexdigest()
				
				# rename the file to its SHA-1
				md = hashlib.sha1(buffer).hexdigest()

				# check if the piece is already cached
				if not os.path.isfile(self.CachePath + "/" + md):
					current_piece = file(self.CachePath + "/" + md, "wb")
					current_piece.write(buffer)
					current_piece.close()

				# update mapfile with hash
				mapfile.write(md + '\n')

				# append piece to return list
				piecelist.append(md)

				buffer = source.read(self.PieceSize*1024)

			mapfile.close()
			source.close()

		# Loop through a whole directory
		elif os.path.isdir(path):
			parselist = os.listdir(path)

			# if settings say to keep empty directories and the directory is empty:
			if self.PreserveEmptyFolders == True and len(parselist) == 0:
				mapfile = file(mappath, "a")
				# print the path to the map file
				mapfile.write(path + "/" + '.coldplaceholder' + '\n')
				mapfile.close()

			for f in parselist:
				if os.access(path+"/"+f, os.R_OK) or os.path.isdir(path+"/"+f):
					piecelist.extend(self.CreateMap(path+"/"+f, mappath))
				else:
					print "WARNING: cannot access %s; skipping" % path+"/"+f
		else:
			print "WARNING: skipping unknown file type: %s" % path

		return piecelist

#	CreateMap = Callable(CreateMap)




	# Purpose:  given a server list and a piece name, return a list of all the servers
	# 			in the list (a subset) for which that piece is a file
	#
	# Called by SendToCloud(path)
	# Called indirectly by -s [--send] flag
	# returns:
	#	RepositoryServer[]	list of server objects that have the piece available
	def LookupPiece(self, piecename, serverlist=[]):
		result_server_list = []

		# if empty optional serverlist, default to self.ServerList:
		if len(serverlist) == 0:
			serverlist = self.ServerList

		print "(LookupPiece!)"
		
		for s in serverlist:
			print "(LookupPiece) IsFile %s: %s" % (s.get_host(), piecename)
			if s.IsFile(piecename) == True:
				result_server_list.append(s)
				

		return result_server_list
#	LookupPiece = Callable(LookupPiece)


	# Purpose:  given a server list and a filename path, return a file result structure
	#			of all the servers in the list (a subset) for which that path is a file
	#
	# returns:
	#	(servers, downpieces)	tuple: first is list of servers available using
	#			redundancy prefs needed to get all pieces, default random. second is
	#			list of pieces that are unavailable

	def LookupFile(self, serverlist, mappath, filename=''):

		# todo make a bunch of threads to call a remote ls on all the servers,
		#  then concatenate the results
		# but for now we'll make do with a serial algorithm

		#res = LookupResultStruct()
		uppieces = list()
		downpieces = list()

		# assert existence
		if (os.path.isfile(self.MapPath + "/" + mappath) == False) and (os.path.isfile(self.MapPath + "/" + mappath + ".map") == False):
			print "ERROR: LookupFile() map file not found: " + mappath
			errorfiles.append(mappath)
			return errorfiles

		# if we must append a ".map" to the filename, do so:
		if (os.path.isfile(self.MapPath + "/" + mappath) == False) and (os.path.isfile(self.MapPath + "/" + mappath + ".map") == True):
			if self.DebugOutput == True:
				print "appending .map to filename"
			mappath = mappath + ".map"

		# read entire map file into memory
		mapfile = file(self.MapPath + "/" + mappath, "r")
		maplines = mapfile.readlines()
		mapfile.close()

		# working file information
		currentfilename = ""
		# list of piece names, which are all MD5s
		pieces = []
		# list of lists.  outer list corresponds to each piece in pieces[],
		#  inner list is of hosting servers
		availabilitylist = []
		# 'unavailable' flag is set in the lookup loop below if a piece has been
		#  identified as missing, so the rest of the file is ignored
		unavailable = False

		# strip maplines[] strings of extra whitespace
		i=0
		while i < len(maplines):
			maplines[i] = maplines[i].strip()
			i=i+1

		# iterate through map file lines
		i=0
		while i < len(maplines):
			#debug
			print "processing line: " + maplines[i]

			# if the line is a name:
			if len(maplines[i]) > 1 and IsValidSHA1(maplines[i]) == False:

				#debug
				print "name line: " + maplines[i]

				# save the current file state to res:
				if len(currentfilename) > 0:
					res.filecount = res.filecount+1


				# if filename is not empty, verify it matches:
				if (len(filename) == 0) or (maplines[i] == filename):
					# set the current file state
					print "looking up next file: " + maplines[i]
					currentfilename = maplines[i]
					pieces = []
					availability = []
					unavailable = False

				i=i+1
				continue

			# if the line is a SHA-1:
			elif len(maplines[i]) > 0 and IsValidSHA1(maplines[i]) == True:
				pieces.append(maplines[i])
				#debug
				print "looking up piece: " + maplines[i]

				# get piece availability
				#servers = self.LookupPiece(maplines[i], s)
				servers = self.LookupPiece(maplines[i], self.ServerList)

				# if not available:
				if len(servers) < 1:
					# print 'piece unavailable' message
					print "ERROR: piece %s unavailable for file %s; skipping" % (maplines[i], currentfilename)
					unavailable = True
				# if available:
				else:
					# append servers (a list) to availabilitylist
					availabilitylist.append(servers);


		return res
	#	LookupFile = Callable(LookupFile)


	# Purpose:  given a server list and a map, return availability
	#
	# returns:
	#
	def LookupMap(self, serverlist, path):
		result_server_list = []

		for s in serverlist:
			if s.IsFile(path) == True:
				result_server_list.append(s)
		#		print "(FindPiece) IsFile %s: %s" % (s.get_host(), path)

		return result_server_list
#	LookupMap = Callable(LookupMap)



	# Purpose:  given a filename, mappath, return list of pieces needed to recreate filename
	# Returns:  list of MD5s, which are piece names.
	#		   empty list on error.
	def ListFilePieces(self, mappath, filename):
	   # return variable, list of strings of MD5s
		piecelist = []

		# if we must append a ".map" to the filename, do so:
		if (os.path.isfile(self.MapPath + "/" + mappath) == False) and (os.path.isfile(self.MapPath + "/" + mappath + ".map") == True):
			if self.DebugOutput == True:
				print "appending .map to filename"
			mappath = mappath + ".map"

		# read entire map file into memory
		mapfile = file(self.MapPath + "/" + mappath, "r")
		maplines = mapfile.readlines()
		mapfile.close()

		# strip maplines[] strings of extra whitespace
		i=0
		while i < len(maplines):
			maplines[i] = maplines[i].strip()
			i=i+1

		# bool indicating whether currently iterating file is the one we want to collect pieces from
		filematch = False

		# iterate through map file lines
		i=0
		while i < len(maplines):
			# cache this function call, since it's constant for the loop
			validhash=IsValidSHA1(maplines[i])

			# if the line is a name:
			if len(maplines[i]) > 1 and validhash == False:
				# if name matches:
				if maplines[i] == filename:
					# set the flag to indicate we are on the right file
					filematch = True
				else:
					# We're done matching
					filematch = False
			# if the line is a SHA-1, and we're on the right file:
			elif len(maplines[i]) > 1 and validhash == True and filematch == True:
				piecelist.append(maplines[i])
			i=i+1

		return piecelist


	# Purpose:  convert a mappath into a list of filenames, which are strings
	#
	# Returns:  list of strings: filenames
	def ListMapFiles(self, mappath):
		# return variable, list of strings of filenames
		filenames = list()

		# if we must append a ".map" to the filename, do so:
		if (os.path.isfile(self.MapPath + "/" + mappath) == False) and (os.path.isfile(self.MapPath + "/" + mappath + ".map") == True):
			if self.DebugOutput == True:
				print "appending .map to filename"
			mappath = mappath + ".map"

		# read entire map file into memory
		mapfile = file(self.MapPath + "/" + mappath, "r")
		maplines = mapfile.readlines()
		mapfile.close()

		# strip maplines[] strings of extra whitespace
		i=0
		while i < len(maplines):
			maplines[i] = maplines[i].strip()
			i=i+1

		# iterate through map file lines
		i=0
		while i < len(maplines):
			# if the line is a name:
			if len(maplines[i]) > 1 and IsValidSHA1(maplines[i]) == False:
				# if the file name is not a placeholder for an empty directory:
				if os.path.basename(maplines[i]) != '.coldplaceholder':
					filenames.append(maplines[i])
			i=i+1

		return filenames


	# Purpose:  convert a mappath into a list of piece names, which are strings of SHA1 hashes.
	#			shorthand function for ListMapFiles()...ListFilePieces()
	#
	# Returns:  list of strings: piece names (SHA1 hashes)
	def ListMapPieces(self, mappath):
		# return variable
		mappieces = []

		# get files from map
		mapfiles = self.ListMapFiles(mappath)

		# get pieces from files
		for f in mapfiles:
			mappieces.extend(self.ListFilePieces(mappath, f))

		return mappieces



	# Purpose:  given a list of pieces and an optional server list, split into
	#		   two lists, available and unavailable
	# Arguments:	(optional) serverlist[]: defaults to self.ServerList, list of
	#			   repositoryservers to reference
	#			   piecelist[]:	list of strings which are SHA1 hashes
	# Returns:
	#   (available[], unavailable[])	tuple of lists of strings, which are piece names
	def PieceSplitByAvail(self, piecelist, serverlist=[]):
		# return variables, returned as a tuple
		available = []
		unavailable = []

		# if zero pieces, return empty:
		if len(piecelist) < 1:
			return (available, unavailable)

		# if no optional servers provided, default to self.ServerList
	#	if len(serverlist) < 1:
	 #	   serverlist = self.ServerList
		# if no optional servers provided, leave it empty so the lookups
		#   default to self.ServerList

		# iterate through piecelist, split into two lists
		for p in piecelist:
			# if servers found hosting the piece:
			if len(self.LookupPiece(p, serverlist) > 0):
				available.append(p)
			# else, it's offline:
			else:
				unavailable.append(p)

		return (available, unavailable)


	# Purpose:  given a filename, mapname, and an optional server list, split into
	#		   two lists, available and unavailable pieces
	# Arguments:	(optional) serverlist[]: defaults to self.ServerList, list of
	#				   repositoryservers to reference
	#			   mappath:	name of the map file, without ".map" extension
	#			   filename:   name of the file to query pieces of
	# Returns:
	#   (available[], unavailable[])	tuple of lists of strings, which are piece names
	def FileSplitByAvail(self, mappath, filename, serverlist=[]):
		# return variables, returned as a tuple
		available = []
		unavailable = []

		# default serverlist to self.ServerList
		if len(serverlist) < 1:
			severlist = self.ServerList

		for f in self.ListMapFiles(mappath):
			avail, unavail = self.PieceSplitByAvail(list(p in self.ListFilePieces(mappath, f)), serverlist)
			#for p in self.ListFilePieces(mappath, f):
			 #   avail, unavail = self.PieceSplitByAvail(serverlist, list(p))


		return (available, unavailable)


	# PURPOSE:	given a piece name (SHA1 hashes), either duplicate it to meet minimum
	#			redundancy, or delete copies to meet maximum redundancy.
	# ARGUMENTS:
	#	piecename	(string) name of piece, always a SHA1 hashes
	#
	# RETURNS:
	#	Integer		number of copies available after copying/deleting
	def UpdatePiece(self, piecename):

		piecename = piecename.strip()
		availableservers = self.LookupPiece(piecename)

		# if it is unavailable, return:
		if len(availableservers) == 0:
			# here zero indicates zero available copies, not a boolean success
			return 0

		# if we don't have enough available:
		elif len(availableservers) < self.MinimumRedundancy:

			# addcount is number we are short
			addcount = (self.MinimumRedundancy - len(availableservers))

			if self.DebugOutput == True:
				print "adding %d copies of piece %s" % (addcount, piecename)

			# create server list containing all the servers that do not
			# already have the piece
			choiceservers = self.GetComplementServerList(availableservers)

			# select which servers to copy the piece to
			selectedservers = self.GetNRedundancyServers(choiceservers, addcount)

			# Debug: dump server list results
			if self.DebugOutput == True:
				print "with %s: " % piecename
				for a in availableservers:
					print a.get_host()
				print "without %s: " % piecename
				for c in choiceservers:
					print c.get_host()
				print "selected for %s: " % piecename
				for s in selectedservers:
					print s.get_host()

			# Get one copy of the file from a random server that has it to send to the other hosts
			# Note: this will eventually be replaced with sending a command to a host with
			#		the piece so that the piece is transferred within the cloud
			sourceserver = self.GetNRedundancyServers(availableservers, 1)
			sourceserver.ReceiveFile(sourceserver.get_path() + "/" + piecename, self.CachePath.strip() + "/" + piecename)


			# Send file to hosts
			for s in selectedservers:
				s.SendFile(s.get_path() + "/" + piecename, self.CachePath.strip() + "/" + piecename)

			# Return new total number of copies
			return len(availableservers) + addcount



		elif len(availableservers) > self.MaximumRedundancy:

			# remcount is number we are over maximum
			remcount = (self.MaximumRedundancy - len(availableservers))

			if self.DebugOutput == True:
				print "removing %d copies of piece %s" % (remcount, piecename)

			# select which servers to copy the piece to
			selectedservers = self.GetNRedundancyServers(availableservers, addcount)

			for s in selectedservers:
				if self.DebugOutput == True:
					print "removing from host: " + s.get_host()
				s.RemoveFile(s.get_path() + "/" + piecename)

			# Return new total number of copies
			return len(availableservers) - remcount


	# PURPOSE:	given a map path, update each contained piece to meet
	#			redundancy specs by calling self.UpdatePiece()
	# ARGUMENTS:
	#	mappath				(string)
	#
	# RETURNS:
	#	(List, List)		( [list of strings] names of pieces that are unavailable
	#						  [list of strings] names of files that are not completely available )
	def UpdateMap(self, mappath):

		# return variables
		totaldownpieces = []
		totaldownfiles = []

#		# get pieces
#		mappieces = self.ListMapPieces(mappath)
#		uppieces, downpieces = self.PieceSplitByAvail(mappieces)

		# get files
		mapfiles = self.ListMapFiles(mappath)

		for f in mapfiles:
			uppieces, downpieces = self.FileSplitByAvail(mappath, f)

			if len(downpieces) > 0:
				# add count to return var
				totaldownpieces.extend(downpieces)

				# add file to down list
				totaldownfiles.append(f)

			# update each available piece, even if file is not totally available
			for p in uppieces:
				self.UpdatePiece(p)

		# return tuple of list of strings
		return (totaldownpieces, totaldownfiles)






	# Purpose:
	#	Send 'path' to the cloud by chopping into pieces, making a map,
	#	and distributing pieces
	# Called by -s [--send] flag
	def SendToCloud(self, path):

		PieceList = self.CreateMap(path)
		
		for p in PieceList:
				p = p.strip()

		if self.DebugOutput == True:
			print "SendToCloud() PieceList: "
			for p in PieceList:
				print "  " + p

		if self.RedundancyOrdering == "usage-proportional":
			for p in PieceList:
				for s in self.ServerList:
					print "%s bounds: [%x, %x]" % (s.get_host(), s.HashSpaceLowerBound, s.HashSpaceUpperBound)
					if s.HashSpaceLowerBound <= int(p,16) and int(p,16) <= s.HashSpaceUpperBound:
						# copy piece to 's'
						print "sending %x to %s!" % (int(p,16), s.get_host())
						s.SendFile(s.get_path() + "/" + p, self.CachePath.strip() + "/" + p)
						if self.DebugOutput == True:
							print "Invalidating DfCache: " + s.get_host()
							s.DfCacheIsCurrent = False
				print
				
		
		
		if self.RedundancyOrdering == "random":
			for p in PieceList:
				LocalServerList = []

				for s in self.ServerList:
					LocalServerList.append(s)

				# query all servers and count existing piece copies
				ExistingCopies = self.LookupPiece(p, LocalServerList)
	#			print "ExistingCopies [SFTC]: "
	#			for c in ExistingCopies:
	#				print c.get_host().strip()
				ExistingCount = len(ExistingCopies)

				# for each server that already has a copy:
				#	remove that server from LocalServerList (so we don't try to send another copy to it)
				for e in ExistingCopies:
					for l in LocalServerList:
						if l.get_host() != e.get_host() or l.get_user() != e.get_user() or l.get_path() != e.get_path():
							continue
						else:
							LocalServerList.remove(e)

				# copy each piece to as many servers as specified as minimum redundancy minus the number that already exist
	#			DestinationServers = GetNRedundancyServers(ServerList, RedundancyOrdering, MinimumRedundancy-ExistingCount)

				# shuffle the server list
				random.shuffle(LocalServerList)

				# send the current piece to (MinimumRedundancy-ExistingCount) servers,
				# but don't pick more servers than we have available
				if (self.MinimumRedundancy - ExistingCount) > 0:
					for i in range(min(self.MinimumRedundancy - ExistingCount, len(LocalServerList))):
						# Verify free space requirement before actually sending
						if LocalServerList[0].Df()/1024 < float(self.ServerFreeMinMB):
							print "WARNING: insufficient space: %s, %f < %d" % (LocalServerList[0].get_host(), LocalServerList[0].Df()/1024, float(self.ServerFreeMinMB))
						if self.DebugOutput == True:
							print "sending %s to %s\n" % (p.strip(), LocalServerList[0].get_host().strip())
					# top priority todo: replace this function with a repositoryserver call that handles df
			#			SendFileToServer(LocalServerList[0].get_host(), LocalServerList[0].get_user(), LocalServerList[0].get_path() + "/" + p, self.CachePath.strip() + "/" + p)
						LocalServerList[0].SendFile(LocalServerList[0].get_path() + "/" + p, self.CachePath.strip() + "/" + p)

						# Iterate through the live server list and unset its DfCache, since we're changing the free space
						i = 0
						for s in self.ServerList:
							if s == LocalServerList[0]:
								if self.ServerList[i].DfCacheIsCurrent == True:
									if self.DebugOutput == True:
										print "Invalidating DfCache: " + self.ServerList[i].get_host()
									self.ServerList[i].DfCacheIsCurrent = False
								break
							i += 1

						LocalServerList.pop(0)


	# PURPOSE:
	# Collect pieces listed in <mappath> and reassemble them into <destpath>
	#  if namelist is not empty, only the names listed are restored.
	#
	# NOTES:
	# If options are implemented, the function can recurse (iterate?) until all the files
	#  are retrieved, outputing status and stuff while it goes.
	#
	# RETURNS:
	#	list of error files
	def ReceiveFromCloud(self, mappath, destpath = '', namelist = []):

		# return data, list of files with errors
		errorfiles = []

		# assert existence
		if (os.path.isfile(self.MapPath + "/" + mappath) == False) and (os.path.isfile(self.MapPath + "/" + mappath + ".map") == False):
			print "ERROR: ReceiveFromCloud() map file not found: " + mappath
			errorfiles.append(mappath)
			return errorfiles

		# if we must append a ".map" to the filename, do so:
		if (os.path.isfile(self.MapPath + "/" + mappath) == False) and (os.path.isfile(self.MapPath + "/" + mappath + ".map") == True):
			if self.DebugOutput == True:
				print "appending .map to filename"
			mappath = mappath + ".map"

		# if the destpath is supplied, then
		# if the destination is an existing file, change it to the file's path
		#  so it's the directory where the file is
		if len(destpath) > 0:
			if os.path.isfile(destpath) == True:
				desthpath = os.path.dirname(destpath)
			elif os.path.isdir(destpath) == False:
				print "ERROR: ReceiveFromCloud() destination path not found: " + destpath
				errorfiles.append(destpath)
				return errorfiles
		# if destpath is not supplied, default to the class's Outpath
		else:
			# if the class option exists, use it:
			if len(self.Outpath) > 0:
				destpath = self.Outpath
				print "destpath := " + self.Outpath
				if not os.path.exists(self.Outpath):
					os.makedirs(self.Outpath)
			# otherwise, return an error
			else:
				print "ERROR: ReceiveFromCloud() destination path not found: " + self.Outpath
				errorfiles.append(self.Outpath)
				return errorfiles

		#debug
		print "using destpath: " + destpath


		# read entire map file into memory
		mapfile = file(self.MapPath + "/" + mappath, "r")
		maplines = mapfile.readlines()
		mapfile.close()

		tempfilepath = tempfile.mkdtemp()

		if self.DebugOutput == True:
			print "using temporary file path: " + tempfilepath

		# working file information
		currentfilename = ""
		# list of piece names, which are all SHA1 hashes
		pieces = []
		# list of lists.  outer list corresponds to each piece in pieces[],
		#  inner list is of hosting servers
		availabilitylist = []
		# 'unavailable' flag is set in the lookup loop below if a piece has been
		#  identified as missing, so the rest of the file is ignored
		unavailable = False

		# strip maplines[] strings of extra whitespace
		i=0
		while i < len(maplines):
			maplines[i] = maplines[i].strip()
			i=i+1

		# iterate through map file lines
		i=0
		while i < len(maplines):

			#debug
			print "processing line: " + maplines[i]

			# if the line is a name:
			if len(maplines[i]) > 1 and IsValidSHA1(maplines[i]) == False:

				#debug
				print "if1 : " + maplines[i]

				# if the file name is a placeholder for an empty directory:
				if os.path.basename(maplines[i]) == '.coldplaceholder':
					#debug
					print "making empty directory: " + destpath + "/" + os.path.dirname(maplines[i])
					# if doesn't already exist (sanity check):
					if not os.path.isdir(destpath + "/" + os.path.dirname(maplines[i])):
						# make the empty folder
						os.makedirs(destpath + "/" + os.path.dirname(maplines[i]))
					i=i+1
					continue

				# and if namelist is not empty, verify the name is somewhere in the list:
				elif (len(namelist) == 0) or (len(namelist) > 0 and namelist.count(maplines[i]) > 0):
					# reset the current file state
					print "looking up next file: " + maplines[i]
					currentfilename = maplines[i]
					pieces = []
					availability = []
					unavailable = False

			# if the line is an SHA1 hashes:
			elif len(maplines[i]) > 0 and IsValidSHA1(maplines[i]) == True:
				pieces.append(maplines[i])
				#debug
				print "looking up piece: " + maplines[i]

				# get piece availability
				servers = self.LookupPiece(maplines[i], self.ServerList)

				# if not available:
				if len(servers) < 1:
					# print 'piece unavailable' message
					print "ERROR: piece %s unavailable for file %s; skipping" % (maplines[i], currentfilename)
					unavailable = True
				# if available:
				else:
					# append servers (a list) to availabilitylist
					availabilitylist.append(servers);

			#debug
	#		print "if: " + str(len(pieces)) + " " + str(unavailable)


			# if this is the last line, and we have pieces, and it's available, collect:
			collect = False
			if i == (len(maplines)-1):
				if len(pieces) > 0 and unavailable == False:
					print "last line: " + maplines[i]
					collect = True
				else:
					collect = False
			else:
				collect = bool(len(pieces) > 0 and unavailable == False and IsValidSHA1(maplines[i+1].strip()) == False)

			# if the next line is all the pieces are available, collect the file:
			if collect:
				# debug
				if self.DebugOutput == True:
					print "collecting file: " + currentfilename + " (" + str(len(pieces)) + " pieces)"

				# copy each piece to our tempfilepath
				for p in pieces:
					# pop the front of the list, which gives us a list of all servers with the piece available
					all_servers = availabilitylist.pop(0)
					
					# debug
					print "found copy count: %d" % len(all_servers)

					# trim the number of servers down to however many we want to copy from simultaneously,
					#  which is hardcoded to 1 for now
					server = self.GetNRedundancyServers(all_servers, 1).pop()

					# debug
					print "downloading piece: " + p

					if server is not None:
						#!wrong! server.ReceiveFromCloud(p, tempfilepath + "/" + p)
						# debug
					#	print "GFFS(): " + server.get_host() + " " + server.get_user() + " " + server.get_path() + "/" + p + " " + tempfilepath + "/" + p
						passes = GetFileFromServer(server.get_host(), server.get_user(), server.get_path() + "/" + p, tempfilepath + "/" + p)

					#	print "passes: " + str(passes)

					# if our ReceiveFromCloud didn't make a sizeable file, error and return
					if os.path.exists(tempfilepath + "/" + p):
						if os.path.getsize(tempfilepath + "/" + p) < 1:
							print "ERROR: piece size 0: " + p
							errorfiles.append(p)
							return errorfiles
					else:
						print "ERROR: does not exist: " + tempfilepath + "/" + p
						errorfiles.append(p)
						return errorfiles

				# concatenate each piece to the final file
				if not os.path.exists(os.path.dirname(destpath + "/" + currentfilename)):
					os.makedirs(os.path.dirname(destpath + "/" + currentfilename))
				outfile = file(destpath + "/" + currentfilename, "wb")
				for p in pieces:
					if self.DebugOutput == True:
						print "writing piece: " + p
					infile = file(tempfilepath + "/" + p)
					outfile.write(infile.read())
					infile.close()
				outfile.close()

				#debug
				print "successfully downloaded: " + destpath + "/" + currentfilename

			i=i+1
		return errorfiles

	# FindFile:
	# ARGUMENTS:
	#	filenameregex:	regex applied to all files in available maps,
	#					matching files are returned
	#	mapregex:		Optional,
	#					only iterate through the map files that match the regex, if given
	# PURPOSE:
	#	given a filename and optionally a list of maps,
	#	return the names of files that contain the filename
	def FindFile(self, filenameregex='[a-zA-Z0-9/\-._]*', mapregex=''):
		# return variable
		# list of matching file names
		matches = []

		# list of map files in mappath that match the regex for iterating through
		mapmatches = []

		# list all maps
		out = os.listdir(self.MapPath)

		# remove the .map extensions
		i=0
		while i < len(out):
			if out[i][len(out[i])-4:] == ".map":
				out[i] = str(out[i][:len(out[i])-4])
			i=i+1

		# get the maps matching the parameter regex, if given:
		# (equivalent to `ls $mappath | grep $mapregex` in bash)
		if len(mapregex) > 0:
			if self.DebugOutput == True:
				print "applying parameter map regex: " + mapregex

			# if the regex doesn't match ".map", remove the ".map" extension
			#	from the filenames before parsing
# 			if re.match(mapregex, ".map") is None or mapregex[len(mapregex)-4:] != ".map":
# 				#debug
# 				print "removing extensions."
# 				# remove the .map extensions
# 				i=0
# 				while i < len(out):
# 					if out[i][len(out[i])-4:] == ".map":
# 						out[i] = str(out[i][:len(out[i])-4])
# 					i=i+1

			for o in out:
				r = re.match(mapregex, o)
				if r is not None:
					mapmatches.append(o)

		# else use the class's stored regex set by argument --map-regex
		elif len(self.FindFileMapRegex) > 0:
			if self.DebugOutput == True:
				print "applying map regex: " + self.FindFileMapRegex

			# if the regex doesn't match ".map", remove the ".map" extension
			#	from the filenames before parsing
			if re.match(self.FindFileMapRegex, ".map") is None or self.FindFileMapRegex[len(self.FindFileMapRegex)-4:] != ".map":
				#debug
#				print "removing extensions."
				# remove the .map extensions
# 				i=0
# 				while i < len(out):
# 					if out[i][len(out[i])-4:] == ".map":
# 						out[i] = str(out[i][:len(out[i])-4])
# 					i=i+1

				for o in out:
					r = re.match(self.FindFileMapRegex, o)
					if r is not None:
						mapmatches.append(o)
		# else don't use a regex
		else:
			mapmatches = out

		print "regex matched maps: ",
		for m in mapmatches:
			print m


		# iterate through each matching map:
		for m in mapmatches:
			#debug
			print "processing map: " + m

			# read map lines into memory
			# add the .map extension that was removed for regex scanning if needed
			if m[len(m)-4:] == ".map":
				mfile = file(self.MapPath + "/" + m, "r")
			else:
				mfile = file(self.MapPath + "/" + m + ".map", "r")
			maplines = mfile.readlines()
			mfile.close()

			# strip maplines[] strings of extra whitespace
			i=0
			while i < len(maplines):
				maplines[i] = maplines[i].strip()
				i=i+1

			# iterate over lines and try to match regex
			for l in maplines:

				# if the line is a name:
				if len(l) > 1 and IsValidSHA1(l) == False:

					#debug
					print "file: " + l

					# apply file name regex
					r = re.match(filenameregex, l)

					# if match:
					if r is not None:
						#debug
						print "matched file: " + l

						# append to return var
						matches.append(m + ":" + l)

		return matches

	# FindMap:
	# ARGUMENTS:
	#	mapregex:		regex applied to all maps in self.MapPath, matching
	#						maps are returned as a list
	# RETURNS:
	#	list:			list of strings, which are names of matching map files
	#						w/o ".map" extension
	# PURPOSE:
	#	given an optional regex, return the names of maps that match
	def FindMap(self, mapregex='[a-zA-Z0-9/\-._]*'):
		matches = []

		# list all maps
		out = os.listdir(self.MapPath)

		# remove the ".map" extensions
		i=0
		while i < len(out):
			if out[i][len(out[i])-4:] == ".map":
				out[i] = str(out[i][:len(out[i])-4])
			i=i+1

		# get the maps matching the parameter regex, if given:
		# (equivalent to `ls $mappath $mapregex` in bash)
		if len(mapregex) > 0:
			if self.DebugOutput == True:
				print "applying map regex: " + mapregex

			for o in out:
				r = re.match(mapregex, o)
				if r is not None:
					mapmatches.append(o)

		return matches
