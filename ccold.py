# Mitch Andrews

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
import shutil
import sqlite3

from ccfroutines import *
from cglobals import *
from SQLiteDataSource import *

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
		
		# obsolete
		#self.MinimumRedundancy = 1
		#self.MaximumRedundancy = 5
		#self.RedundancyOrdering = "random"
		
		self.Redundancy = 1
		self.PrevRedundancy = 1
		self.RedundancyOrdering = "usage-proportional"

		# For 'usage-proportional', ServerList lists redundant servers
		# together, for example, if there is 1 redundant servers among
		# 3 total, A, A, B, with each letter representing a data set,
		# they will be listed in ServerList in the order A, A, B. 
		# 'Redundancy' defines the copy count.
		self.ServerList = []
		
		self.SQLDataSource = SQLiteDataSource()

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

			## Redundancy is now inferred from 'layout.txt' and is set elsewhere ##
			
			# # Redundancy
			# r = re.match(r'^[ \t]*redundancy[ \t]*=[ \t]*(.*)$', j)
			# if r is not None:
				# if r.group(1).lower() != "":
					# #todo integrity checks here
					# self.Redundancy = int(r.group(1))
					# if self.DebugOutput == True:
						# print "setting redundancy: " + str(self.Redundancy)

				# continue

			# Redundancy Ordering
			r = re.match(r'^[ \t]*redundancy[-]?ordering[ \t]*=[ \t]*(.*)$', j)
			if r is not None:
				if r.group(1).lower() != "":
					# todo more integrity checks here
					if r.group(1) == "random":
						self.RedundancyOrdering = "random"
						if self.DebugOutput == True:
							print "setting redundancy ordering: " + self.RedundancyOrdering
					elif r.group(1) == "usage-proportional":
						self.RedundancyOrdering = "usage-proportional"
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
			
		
		
		# Read previous server layout from "layout.txt"
		OptionLines = ShellOutputLines('cat layout.txt')
		for j in OptionLines:
			r = re.match(r'^[ \t]*[^#](.*)$', j)
			if r is not None:
			
				# todo integrity checks here
				#  (or conglomorate environment sanity check after loading file)

				# regex to gather hostname
				r = re.split('[ \t@:]+', j)
				if len(r[0]) == 0:
					print "ERROR: user in: " + j
					sys.exit()
				if len(r[1]) == 0:
					print "ERROR: host in: " + j
					sys.exit()
				if len(r[2]) == 0:
					print "ERROR: path in: " + j
					sys.exit()
					
				#print "Attempting to add server! %s %s %s %s %s %s" % (r[0], r[1], r[2], r[3], r[4], r[5])

				rs = RepositoryServer()
				rs.set_user(r[0])
				rs.set_host(r[1])
				rs.set_path(r[2])
				
				if len(r[3]) > 0:
					rs.Band = int(r[3])
					if (rs.Band + 1) > self.Redundancy:
						self.Redundancy = (rs.Band + 1)
					
				if len(r[4]) > 0:
					rs.HashSpaceLowerBound = int(r[4], 16)
				if len(r[5]) > 0:
					rs.HashSpaceUpperBound = int(r[5], 16)

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

#	LoadOptions = Callable(LoadOptions)


	# PURPOSE: Add a server to the unassigned list.  Must call ConsolidateLayout()
	#			manually to include in lineup and activate
	#
	#	* server_string format: user@host:path
	def AddServer(self, server_string):
		server_string = server_string.strip()
		
		r = re.split('[@:]+', server_string)
		if len(r[0]) == 0:
			print "ERROR: user in: " + server_string
			sys.exit()
		if len(r[1]) == 0:
			print "ERROR: host in: " + server_string
			sys.exit()
		if len(r[2]) == 0:
			print "ERROR: path in: " + server_string
			sys.exit()
			
		# Check if it already exists
		for s in self.ServerList:
			if s.get_user() == r[0] and s.get_host() == r[1] and s.get_path() == r[2]:
				print "Warning: Server already exists; skipping!"
				return 1

		rs = RepositoryServer()
		rs.set_user(r[0])
		rs.set_host(r[1])
		rs.set_path(r[2])

		# Verify passwordless access to the server
		if HasPasswordlessSSH(rs.get_host(), rs.get_user()):
#			print "SSH is passwordless!"
			MuteSSHLogin(rs.get_host(), rs.get_user())
		else:
			print "Fatal Error: password required for SSH to %s@%s" % (rs.get_user(), rs.get_host())

		# Check free space in MB, only add server if minimum is met
		MBfree = rs.Df()/1024
		if MBfree < float(self.ServerFreeMinMB):
			print "WARNING: %s space low, not adding: %f MB < %d MB" % (rs.get_host(), MBfree, self.ServerFreeMinMB)
			return 1
			
		self.ServerList.append(rs)
		
		self.ConsolidateLayout(True)

		return 0
		
	
	
	# PURPOSE: Change the band count and consolidate servers
	def ChangeRedundancy(self, newBandCount):
		
		newBandCount = int(newBandCount)
		if newBandCount == self.Redundancy:
			print "WARNING: Redundancy not changed (same); skipping!"
			return 1
		if newBandCount < 1:
			return 2
			
		print "Attention! Changing redundancy requires consolidation, so please make sure all servers are available."
		self.PrevRedundancy = self.Redundancy
		self.Redundancy = newBandCount
		self.ConsolidateLayout(False)
		
		
	# PURPOSE: Move pieces from old layout to new layout within a single band.
	#			Primarily called by ConsolidateLayout()
	#
	#	* given: len(newLayoutLists) > bandNo
	def RepopulateSingleBand(self, newLayoutLists, bandNo=0):
		
		# for each server in old layout in band 'bandNo'
		for s in self.ServerList:
			if s.Band == bandNo:
			
				# get list of servers in new layout that overlap bounds
				# so we can push pieces
				for d in newLayoutLists[bandNo]:
				
					# don't push to self
					if self.ServerList[d] == s:
						continue
						
					# push all pieces from s within d's bounds to d
					s.SendPiecesByTargetRange(self.ServerList[d])
					s.DeletePiecesByRange(self.ServerList[d].HashSpaceLowerBound, self.ServerList[d].HashSpaceUpperBound)
					
	
	def RepopulateBetweenBands(self, newLayoutLists, srcBandNo=0, dstBandNo=1):
		
		# for each server in old layout in band 'bandNo'
		for s in newLayoutLists[srcBandNo]:
			
			# get list of servers in new layout that overlap bounds
			# so we can push pieces
			for d in newLayoutLists[dstBandNo]:
					
				# push all pieces from s within d's bounds to d
				self.ServerList[s].SendPiecesByTargetRange(self.ServerList[d])
		
		
		
	# PURPOSE: Re-organize servers to increase space efficiency, change
	#			 server bounds, and move pieces between servers to match
	def ConsolidateLayout(self, growOnly=True):
		
		if growOnly == False:
		
			# If can't connect, set hash info to -3 and ignore
			upServers = []
			for s in self.ServerList:
				if not IsHostAlive(s.get_host()):
					print "ConsolidateLayout(): host down: %s" % (s.get_host())
					s.Band = -3
					s.HashSpaceLowerBound = -3
					s.HashSpaceUpperBound = -3
				else:
					upServers.append(s)
					
			# Check up servers for minimum free space, otherwise set hash info
			#	to -2 and ignore
			#availServers = []
			availServers = list(upServers)
			for s in upServers:
				if (s.Df()/1024) < self.ServerFreeMinMB:
					print "ConsolidateLayout(): host full: %s" % (s.get_host())
					s.Band = -2
					s.HashSpaceLowerBound = -2
					s.HashSpaceUpperBound = -2
					availServers.remove(s)
				
			# save all previous layout information
			for s in self.ServerList:
				s.PrevRedundancyNo = s.Band
				s.PrevHashSpaceLowerBound = s.HashSpaceLowerBound
				s.PrevHashSpaceUpperBound = s.HashSpaceUpperBound
			
			
			# # Create list of total free space for each Band
			# freeSpace = []
			# curRedundancyNo = 0
			# while curRedundancyNo < self.Redundancy:
				# total = 0
				# for s in availServers:
					# if s.Band == curRedundancyNo:
						# total = total + s.DfCache
				# freeSpace.append(total)
				# curRedundancyNo = curRedundancyNo + 1

			
			# Order available servers by free space, desc
			orderedAvailServers = []
			i=0
			while i < len(availServers) > 0:
				largest_freespace_i = 0
				largest_freespace_size = 0
				
				for s in availServers:
					if orderedAvailServers.count(availServers.index(s)) > 0:
						continue
					if s.DfCache > largest_freespace_size:
						largest_freespace_i = availServers.index(s)
						largest_freespace_size = s.DfCache
						
				print "appending largest index: %d (%d)" % (largest_freespace_i, largest_freespace_size)
				orderedAvailServers.append(largest_freespace_i)
				i = i + 1
			
			# debug output available servers ordered by free space, desc:
			if self.DebugOutput == True:
				print "free space order:"
				for s in orderedAvailServers:
					print "%s %d" % (availServers[s].get_host(), availServers[s].DfCache)
					#print "%s [%d] (%d), " % (availServers[s].get_host(), availServers.index(s), availServers[s].DfCache),
				print ""
				
			# create empty lists for new layout
			newLayoutLists = []
			for x in range(0,self.Redundancy):
				newLayoutLists.append([])
			
			# iterate through available & working servers and set each's
			# new redundancy number
			for s in orderedAvailServers:
			
				# find the index of the layout band with the smallest sum
				smallest_band_i = 0
				smallest_band_size = 9999999999999999999
				for l in newLayoutLists:
					bandSizeSum = 0
					for x in l:
						bandSizeSum = bandSizeSum + availServers[x].DfCache
					if bandSizeSum < smallest_band_size:
						smallest_band_i = newLayoutLists.index(l)
						smallest_band_size = bandSizeSum
					
				# append index to the smallest layout band
				print "adding %s to band %d, new size: %d" % (availServers[s].get_host(), smallest_band_i, smallest_band_size + availServers[s].DfCache)
				newLayoutLists[smallest_band_i].append(s)
				availServers[s].Band = smallest_band_i
				
			# debug print indices of new layout
			if self.DebugOutput == True:
				print "New layout (by index):"
				for l in newLayoutLists:
					print "band %d: " % newLayoutLists.index(l),
					for i in l:
						print " %d" % i,
					print ""
					
			# TODO: touch file indicating consolidation started
			# (for startup sanity checking and mutual exclusion; not implemented)
			# and take server offline
			

			# debug
			for s in self.ServerList:
				print "%s band: %d" % (s.get_host(), s.Band)
					
			# recalculate bounds
			self.RecalculateBounds(newLayoutLists)
			
			# debug print new bounds
			print "New Bounds & All Pieces:"
			for s in self.ServerList:
				s.print_info()
				#s.ListPiecesByRange("4444444444444444444444444444444444444444", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
			
			
			## MOVE PIECES ##
		
			# if number of bands doesn't change, just RepopulateSingleBand() each band
			if self.Redundancy == self.PrevRedundancy:
				if self.DebugOutput == True:
					print "band count unchanged"
					
				for i in range(0, len(newLayoutLists)):
					print "calling RepopulateSingleBand(%d)" % i
					self.RepopulateSingleBand(newLayoutLists, i)
					
			## NOTE: this is severely unoptimized, currently requiring n/2 band copies.
			##			a little effort would easily reduce this to n*log(n), and even
			##			better is probably doable
			
			# if number of bands changed:
			else:
				
				# clear all bands > 0
				for s in self.ServerList:
					if s.Band > 0:
						s.DeletePiecesByRange(0, int("ffffffffffffffffffffffffffffffffffffffff",16))	#defaults to entire range
				# do first duplication:
				
				if self.Redundancy > 1:
					# duplicate to band 1
					print "calling RepopulateBetweenBands(0, 1)"
					self.RepopulateBetweenBands(newLayoutLists, 0, 1)
					
				# if Redundancy > 2, then there are still bands to fill.
				# have band 0 duplicate to evens, band 1 to odds.
				if self.Redundancy > 2:
					for i in range(2, self.Redundancy):
						if i % 2 == 0:
							print "RepopulateBetweenBands(newLayoutLists, 0, %d)" % i
							self.RepopulateBetweenBands(newLayoutLists, 0, i)
						else:
							print "RepopulateBetweenBands(newLayoutLists, 1, %d)" % i
							self.RepopulateBetweenBands(newLayoutLists, 1, i)
				
			
			
			
		# if we are merely appending new servers to existing layout
		else:
			newServer = ""
			for s in self.ServerList:
				if s.Band == -1:
					print "Appending new server: %s" % s.get_host()
					newServer = s
					break
			
			# If can't connect, set hash info to -3 and ignore
			if not IsHostAlive(newServer.get_host()):
				print "ConsolidateLayout(): host down: %s" % (newServer.get_host())
				newServer.Band = -3
				newServer.HashSpaceLowerBound = -3
				newServer.HashSpaceUpperBound = -3
				return 1
			
			# Check up servers for minimum free space, otherwise set hash info
			#	to -2 and ignore
			if (newServer.Df()/1024) < self.ServerFreeMinMB:
				print "ConsolidateLayout(): host full: %s" % (newServer.get_host())
				newServer.Band = -2
				newServer.HashSpaceLowerBound = -2
				newServer.HashSpaceUpperBound = -2
				return 2
				
			# save all previous layout information
			for s in self.ServerList:
				s.PrevRedundancyNo = s.Band
				s.PrevHashSpaceLowerBound = s.HashSpaceLowerBound
				s.PrevHashSpaceUpperBound = s.HashSpaceUpperBound
			
			# find smallest band
			bandSizes = []
			i=0
			while i < self.Redundancy:
				bandSizes.append(0)
				i = i + 1
			for s in self.ServerList:
				if s.Band >= 0:
					bandSizes[s.Band] = bandSizes[s.Band] + s.DfCache
					print "appending size %d (%s) to band %d" % (s.DfCache, s.get_host(), s.Band)
				
			smallest_band_i = 0
			smallest_band_size = 9999999999999999999
			for b in bandSizes:
				if b < smallest_band_size:
					smallest_band_size = b
					smallest_band_i = bandSizes.index(b)
				
			# # debug output band sizes
			# for b in bandSizes:
				# print "band %d size: %d" % (bandSizes.index(b), b)
			# print "smallest band: %d" % smallest_band_i
			
			# append newServer to smallest band
			newServer.Band = smallest_band_i
			
			# recalculate bounds
			self.RecalculateBounds()

			
		## write new layout to file
		self.WriteLayout(newLayoutLists)
			
		## remove lock file
			
			
			
	# PURPOSE: write over previous layout file with new.
	def WriteLayout(self, newLayoutLists):
	
		# remove 'layout.txt'
		os.remove("layout.txt")
	
		# write new info
		for s in self.ServerList:
			layoutFile = file("layout.txt", "a")

			# print the path to the map file
			if s.HashSpaceLowerBound == 0:
				layoutFile.write("%s@%s:%s:%d:0x0000000000000000000000000000000000000000:%s" % (s.get_user(), s.get_host(), s.get_path(), s.Band, str(hex(s.HashSpaceUpperBound))[0:-1]))
			else:
				layoutFile.write("%s@%s:%s:%d:%s:%s" % (s.get_user(), s.get_host(), s.get_path(), s.Band, str(hex(s.HashSpaceLowerBound)[0:-1]), str(hex(s.HashSpaceUpperBound))[0:-1]))
			layoutFile.write('\n')			
	
	
			
	# PURPOSE: after consolidation, recalculate the bounds for all servers
	#
	# * default empty lists implies using existing layout
	def RecalculateBounds(self, newLayoutLists=[]):
		
		# if no new layout provided then use existing
		if len(newLayoutLists) == 0:
			for i in range(0, self.Redundancy):
				newLayoutLists.append([])
			for s in self.ServerList:
				newLayoutLists[s.Band].append(self.ServerList.index(s))
				
			# # debug print layout list
			# for l in newLayoutLists:
				# for s in l:
					# print "band %d: %s" % (newLayoutLists.index(l), s)
		
		for b in newLayoutLists:
			
			# sum available space in this band
			curBand = newLayoutLists.index(b)
			totalSize = 0
			for s in self.ServerList:
				if s.Band == curBand:
					totalSize = totalSize + s.DfCache
					
			print "band %d total size: %d" % (curBand, totalSize)
					
			# update each server in band with new bounds
			hashLowerBound = 0
			for s in b:
				if self.ServerList[s].Band == curBand:
					spacePercentage = (float(self.ServerList[s].DfCache) / float(totalSize))
					hashUpperBound = int(int("ffffffffffffffffffffffffffffffffffffffff", 16) * float(self.ServerList[s].DfCache) / float(totalSize)) + hashLowerBound - 1
					if hashUpperBound > 0xfffffffffffff000000000000000000000000000:
						hashUpperBound = 0xffffffffffffffffffffffffffffffffffffffff
					self.ServerList[s].HashSpaceLowerBound = hashLowerBound
					self.ServerList[s].HashSpaceUpperBound = hashUpperBound
				
					print "%s hashspace percentage: %.4f\nLowerBound: %x\nUpperBound: %x\n" % (self.ServerList[s].get_host(), spacePercentage, hashLowerBound, hashUpperBound)
			
					hashLowerBound = hashUpperBound + 1

			


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
			if s.Band >= 0:
				spacePercentage = float(float(FreeSpace[self.ServerList.index(s)]) / float(sum(FreeSpace)))
				print "space usage: %.4f\nLowerBound: %x\nUpperBound: %x\n" % (spacePercentage, s.HashSpaceLowerBound, s.HashSpaceUpperBound)
			else:
				print "unallocated node, free space: %d" % FreeSpace[self.ServerList.index(s)]
		
			# spacePercentage = float(float(FreeSpace[self.ServerList.index(s)]) / float(sum(FreeSpace)))
			# hashUpperBound = int(int("ffffffffffffffffffffffffffffffffffffffff", 16) * float(float(FreeSpace[self.ServerList.index(s)]) / float(sum(FreeSpace)))) + hashLowerBound - 1
			# s.HashSpaceLowerBound = hashLowerBound
			# s.HashSpaceUpperBound = hashUpperBound
			
			# print "hashspace percentage: %.4f\nLowerBound: %x\nUpperBound: %x\n" % (spacePercentage, hashLowerBound, hashUpperBound)
			
			# hashLowerBound = hashUpperBound + 1

		return FreeSpace, ErrorServers


		
	# List the files contained in all the maps in the maps folder
	# Returns:
	#	list of file names (strings)
	def ListFiles(self):
		# return variable
		filelist = []
		
		## NOT IMPLEMENTED ##

		# # list all maps
		# maps = self.ListMaps()

		# # append files from each map to return variable
		# for m in maps:
			# filelist.extend(self.ListMapFiles(m))

		return filelist


	# Create the map and cache files for the supplied path,
	# returns a list of paths to the pieces
	def CreatePieces(self, path):

		# return value
		piecelist = []

		# remove trailing slash(es)
		path = path.rstrip("/")

		if self.DebugOutput == True:
			print " ## Cold.CreatePieces(): " + path

		# Check if it's a file
		if os.path.isfile(path):
			# Check readability
			if not os.access(path, os.R_OK):
				print "ERROR: can not read: " + path
				sys.exit()

			source = file(path, "rb")

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

				# append piece to return list
				piecelist.append(md)

				buffer = source.read(self.PieceSize*1024)

			source.close()

		return piecelist




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





	# # Purpose:  given a list of pieces and an optional server list, split into
	# #		   two lists, available and unavailable
	# # Arguments:	(optional) serverlist[]: defaults to self.ServerList, list of
	# #			   repositoryservers to reference
	# #			   piecelist[]:	list of strings which are SHA1 hashes
	# # Returns:
	# #   (available[], unavailable[])	tuple of lists of strings, which are piece names
	# def PieceSplitByAvail(self, piecelist, serverlist=[]):
		# # return variables, returned as a tuple
		# available = []
		# unavailable = []

		# # if zero pieces, return empty:
		# if len(piecelist) < 1:
			# return (available, unavailable)

		# # if no optional servers provided, default to self.ServerList
	# #	if len(serverlist) < 1:
	 # #	   serverlist = self.ServerList
		# # if no optional servers provided, leave it empty so the lookups
		# #   default to self.ServerList

		# # iterate through piecelist, split into two lists
		# for p in piecelist:
			# # if servers found hosting the piece:
			# if len(self.LookupPiece(p, serverlist) > 0):
				# available.append(p)
			# # else, it's offline:
			# else:
				# unavailable.append(p)

		# return (available, unavailable)


	# # Purpose:  given a filename, mapname, and an optional server list, split into
	# #		   two lists, available and unavailable pieces
	# # Arguments:	(optional) serverlist[]: defaults to self.ServerList, list of
	# #				   repositoryservers to reference
	# #			   mappath:	name of the map file, without ".map" extension
	# #			   filename:   name of the file to query pieces of
	# # Returns:
	# #   (available[], unavailable[])	tuple of lists of strings, which are piece names
	# def FileSplitByAvail(self, mappath, filename, serverlist=[]):
		# # return variables, returned as a tuple
		# available = []
		# unavailable = []

		# # default serverlist to self.ServerList
		# if len(serverlist) < 1:
			# severlist = self.ServerList

		# for f in self.ListMapFiles(mappath):
			# avail, unavail = self.PieceSplitByAvail(list(p in self.ListFilePieces(mappath, f)), serverlist)
			# #for p in self.ListFilePieces(mappath, f):
			 # #   avail, unavail = self.PieceSplitByAvail(serverlist, list(p))


		# return (available, unavailable)


	# Purpose:
	#	Send 'path' to the cloud by chopping into pieces, creating a db entry,
	#	and distributing pieces
	# Called by -s [--send] flag
	def SendToCloud(self, localPath, dbPath):
		print " ## SendToCloud", localPath, dbPath
		
		# if directory:
		if os.path.isdir(localPath):
		
			dirName = os.path.normpath(localPath).split('/')[-1]
			#dirName = os.path.basename(localPath)
			
			# get directory contents
			dirContents = os.listdir(localPath)
			
			for f in dirContents:
				if f == "." or f == "..":
					continue
					
				# Make subdir in db
				self.SQLDataSource.mkdirs(dbPath + "/" + dirName)
				
				# Recurse to subfiles
				self.SendToCloud(localPath + "/" + f, dbPath + "/" + dirName + "/" + f)
				
			return
			
		# if regular file:
		elif os.path.isfile(localPath):
		
			fileName = os.path.normpath(localPath).split('/')[-1]
			pathName = os.path.dirname(os.path.normpath(localPath))
		
			PieceList = self.CreatePieces(localPath)
			
			for p in PieceList:
				p = p.strip()
					
			# Write DB entries for file & pieces
			self.SQLDataSource.createFile(pathName + "/" + fileName, PieceList)

			if self.DebugOutput == True:
				print "SendToCloud() PieceList: "
				for p in PieceList:
					print "  " + p

			if self.RedundancyOrdering == "usage-proportional":
				for p in PieceList:
					for s in self.ServerList:
						#print "%s bounds: [%x, %x]" % (s.get_host(), s.HashSpaceLowerBound, s.HashSpaceUpperBound)
						if s.HashSpaceLowerBound <= int(p,16) and int(p,16) <= s.HashSpaceUpperBound:
							# copy piece to 's'
							print "Uploading %x to %s [%x, %x]" % (int(p,16), s.get_host(), s.HashSpaceLowerBound, s.HashSpaceUpperBound)
							s.SendFile(s.get_path() + "/" + p, self.CachePath.strip() + "/" + p)
							#if self.DebugOutput == True:
								#print "Invalidating DfCache: " + s.get_host()
							s.DfCacheIsCurrent = False
					print
				
		return
				
		
		
		# if self.RedundancyOrdering == "random":
			# for p in PieceList:
				# LocalServerList = []

				# for s in self.ServerList:
					# LocalServerList.append(s)

				# # query all servers and count existing piece copies
				# ExistingCopies = self.LookupPiece(p, LocalServerList)
	# #			print "ExistingCopies [SFTC]: "
	# #			for c in ExistingCopies:
	# #				print c.get_host().strip()
				# ExistingCount = len(ExistingCopies)

				# # for each server that already has a copy:
				# #	remove that server from LocalServerList (so we don't try to send another copy to it)
				# for e in ExistingCopies:
					# for l in LocalServerList:
						# if l.get_host() != e.get_host() or l.get_user() != e.get_user() or l.get_path() != e.get_path():
							# continue
						# else:
							# LocalServerList.remove(e)

				# # copy each piece to as many servers as specified as minimum redundancy minus the number that already exist
	# #			DestinationServers = GetNRedundancyServers(ServerList, RedundancyOrdering, MinimumRedundancy-ExistingCount)

				# # shuffle the server list
				# random.shuffle(LocalServerList)

				# # send the current piece to (MinimumRedundancy-ExistingCount) servers,
				# # but don't pick more servers than we have available
				# if (self.MinimumRedundancy - ExistingCount) > 0:
					# for i in range(min(self.MinimumRedundancy - ExistingCount, len(LocalServerList))):
						# # Verify free space requirement before actually sending
						# if LocalServerList[0].Df()/1024 < float(self.ServerFreeMinMB):
							# print "WARNING: insufficient space: %s, %f < %d" % (LocalServerList[0].get_host(), LocalServerList[0].Df()/1024, float(self.ServerFreeMinMB))
						# if self.DebugOutput == True:
							# print "sending %s to %s\n" % (p.strip(), LocalServerList[0].get_host().strip())
					# # top priority todo: replace this function with a repositoryserver call that handles df
			# #			SendFileToServer(LocalServerList[0].get_host(), LocalServerList[0].get_user(), LocalServerList[0].get_path() + "/" + p, self.CachePath.strip() + "/" + p)
						# LocalServerList[0].SendFile(LocalServerList[0].get_path() + "/" + p, self.CachePath.strip() + "/" + p)

						# # Iterate through the live server list and unset its DfCache, since we're changing the free space
						# i = 0
						# for s in self.ServerList:
							# if s == LocalServerList[0]:
								# if self.ServerList[i].DfCacheIsCurrent == True:
									# if self.DebugOutput == True:
										# print "Invalidating DfCache: " + self.ServerList[i].get_host()
									# self.ServerList[i].DfCacheIsCurrent = False
								# break
							# i += 1

						# LocalServerList.pop(0)


	# PURPOSE:
	# Collect pieces listed in <mappath> and reassemble them into <destPath>
	#  if namelist is not empty, only the names listed are restored.
	#
	# NOTES:
	# If options are implemented, the function can recurse (iterate?) until all the files
	#  are retrieved, outputing status and stuff while it goes.
	#
	# RETURNS:
	#	list of error files
	#def ReceiveFromCloud(self, destPath, destPath):
	def ReceiveFromCloud(self, dbPath, destPath):
		print " ## ReceiveFromCloud:", dbPath, destPath

		# return data, list of files with errors
		errorfiles = []

		# if the destPath is supplied, then
		# if the destination is an existing file, change it to the file's path
		#  so it's the directory where the file is
		if len(destPath) > 0:
			if os.path.isfile(destPath) == True:
				desthpath = os.path.dirname(destPath)
			elif os.path.isdir(destPath) == False:
				print "ERROR: ReceiveFromCloud() destination path not found: " + destPath
				errorfiles.append(destPath)
				return errorfiles
		# if destPath is not supplied, default to the class's Outpath
		else:
			# if the class option exists, use it:
			if len(self.Outpath) > 0:
				destPath = self.Outpath
				print "destPath := " + self.Outpath
				if not os.path.exists(self.Outpath):
					os.makedirs(self.Outpath)
			# otherwise, return an error
			else:
				print "ERROR: ReceiveFromCloud() destination path not found: " + self.Outpath
				errorfiles.append(self.Outpath)
				return errorfiles

		#debug
		print " ## ReceiveFromCloud Destination path:", destPath

		tempfilepath = tempfile.mkdtemp()

		if self.DebugOutput == True:
			print " ## ReceiveFromCloud using temporary file path:", tempfilepath

		# 'unavailable' flag is set in the lookup loop below if a piece has been
		#  identified as missing, so the rest of the file is ignored
		unavailable = False
		
	
		
		# get id of dbPath
		(id, type) = self.SQLDataSource.getId(dbPath)
		
		# if directory, list contents and recurse
		if type == 1:
			print " ## ReceiveFromCloud processing folder:", dbPath
			
			dirName = os.path.normpath(dbPath).split('/')[-1]
			
			# Iterate through contents, expanding each
			dirContents = self.SQLDataSource.ls(dbPath)
			for f in dirContents:
				if not os.path.exists(destPath + "/" + dirName):
					os.makedirs(destPath + "/" + dirName)
				self.ReceiveFromCloud(dbPath + "/" + f, destPath + "/" + dirName)
			
			
		
		# else if file, gather file
		elif type == 0:
			print " ## ReceiveFromCloud processing file:", dbPath
		
			fileName = os.path.normpath(dbPath).split('/')[-1]
			
			# get piece list
			pieceList = self.SQLDataSource.listFilePieces(dbPath)
			
			# get pieces
			for p in pieceList:
				
				print " ## ReceiveFromCloud processing piece:", p
				
				# generate list of servers hosting 'p'
				serverAlternatives = []
				
				for s in self.ServerList:
					#print "%s bounds: [%x, %x]" % (s.get_host(), s.HashSpaceLowerBound, s.HashSpaceUpperBound)
					if s.HashSpaceLowerBound <= int(p,16) and int(p,16) <= s.HashSpaceUpperBound:
						serverAlternatives.append(s)
						
				# trim the number of servers down to however many we want to copy from simultaneously,
				#  which is hardcoded to 1 for now
				server = self.GetNRedundancyServers(serverAlternatives, 1).pop()
				
				if server is not None:
					passes = GetFileFromServer(server.get_host(), server.get_user(), server.get_path() + "/" + p, tempfilepath + "/" + p)
		
					if os.path.exists(tempfilepath + "/" + p):
					
						if os.path.getsize(tempfilepath + "/" + p) < 1:
							print "ERROR: piece size 0: " + p
							errorfiles.append(p)
							unavailable = True
							
						# it's hard to tell, but right here is the non-error path out of these 'if's.. :P
						
					else:
						print "ERROR: does not exist: " + tempfilepath + "/" + p
						errorfiles.append(p)
						unavailable = True
				else:
					print "ERROR: no server found: " + tempfilepath + "/" + p
					errorfiles.append(p)
					unavailable = True
						
						
			# If all the pieces copied in correctly
			if not unavailable:
			
				# concatenate each piece to the final file
				if not os.path.exists(os.path.dirname(destPath + "/" + fileName)):
					os.makedirs(os.path.dirname(destPath + "/" + fileName))
				outfile = file(destPath + "/" + fileName, "wb")
				for p in pieceList:
					if self.DebugOutput == True:
						print "writing piece: " + p
					infile = file(tempfilepath + "/" + p)
					outfile.write(infile.read())
					infile.close()
				outfile.close()

				#debug
				print "successfully downloaded: " + destPath + "/" + fileName
				
				
			## REMOVE LOCAL PIECES ##
			try:
				shutil.rmtree(tempfilepath)
			except OSError, e:
				pass
			
						
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
