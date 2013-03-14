# Mitch Andrews
# 11/15/10

# cold.globals.py program dependencies:
#  global imports (sys, os, et al)
#
#  cfroutines


## Functionality that might be missing:
#	* on server add, clear motd if possible, or notify
#	* on server add, make repository folder if necessary

import sys
import os
import subprocess
import re
from ccfroutines import *

class RepositoryServer:

	# Member Variables:
	#
	# String user
	# String host
	# String path
	# String PieceCache[]
	# Boolean CacheIsCurrent (default = False)
	# %Integer pingresults[]
	# Integer DfCache
	# DfCacheIsCurrent (default = False)
	# Integer ServerFreeMB (default = 2048)
	# String HashSpaceLowerBound
	# String HashSpaceUpperBound

	def __init__(self):
		self.PieceCache = []
		self.CacheIsCurrent = False
		self.DfCache = -1
		self.DfCacheIsCurrent = False
		self.HashSpaceLowerBound = -1
		self.HashSpaceUpperBound = -1

	def __eq__(self, s):
		if self.host == s.get_host() and self.user == s.get_user() and self.path == s.get_path():
			return True
		return False

	def set_user(self, username):
		self.user = username.strip()
	def set_host(self, hostname):
		self.host = hostname.strip()
	def set_path(self, repositorypath):
		self.path = repositorypath.strip()
	def get_user(self):
		return self.user.strip()
	def get_host(self):
		return self.host.strip()
	def get_path(self):
		return self.path.strip()
	def SetHashSpaceLowerBound(self, lowerBound):
		self.HashSpaceLowerBound = lowerBound
	def SetHashSpaceUpperBound(self, upperBound):
		self.HashSpaceUpperBound = upperBound

	def print_info(self):
		print "=== %s@%s:%s ===" % (self.user, self.host, self.path)




	# df returns the number of KB available at <path>
	# returns:
	#	Integer		free KB on server at <path>
	#	-1			error
	def Df(self):

		if self.DfCacheIsCurrent == True:
			return self.DfCache

		if (self.host == "" or self.user == "" or self.path == ""):
			return -1

		# open ssh connection
		p = subprocess.Popen(['ssh', '-T', '-q', self.user + '@' + self.host], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

		# df -B 1024 <path> | tail -n +2 | awk '{print $4}'
		output,err = p.communicate('df -B 1024 %s | tail -n +2 | awk \'{print $4}\'' % self.path)

	#	p.terminate()

		# debug
		#print "df output: " + output.strip()
		#print "df error: " + err.strip()

		if (len(output) == 0 or int(output) < 0):
			return -1

		# Update the cache
		self.DfCache = int(output.strip())
		self.DfCacheIsCurrent = True

		return self.DfCache

	
	# Modify the server's DfCache based on a local file which is presumed
	#  to be sent to the server.  This reduces free space
	def DfConsumeFile(self, path):
		pass


	# Modify the server's DfCache based on a local file which is presumed
	#  to be deleted from the server.  This increases free space
	def DfFreeFile(self, path):
		pass


	# Purpose: given a path, determine if it exists and is a normal file,
	#			this is done by querying the cache, and updating it if
	#			necessary
	# Returns:
	#	Boolean		True if <path> is a normal file relative to self.path, False otherwise
	def IsFile(self, path):

		if self.CacheIsCurrent == True:
			# iterate over each cache piece
			for p in self.PieceCache:
				if p == path:
					return True
			return False

		else:
	#		if self.DebugOutput == True:
			print "IsFile() Updating cache..."

			# Update self.PieceCache and self.CacheIsCurrent
			output,err = serverls(self.get_host(), self.get_user(), self.get_path())

			if len(output) > 10000:
				print "IsFile() ERROR: serverls(host=%s) returned %d results (>10000)" % (self.get_host(), len(output))
				sys.exit()

			self.PieceCache = output
			self.CacheIsCurrent = True

	 #		if self.DebugOutput == True:
			print "IsFile() Updated Piece Cache %s:" % self.get_host()
			for p in self.PieceCache:
				print "  " + p


			# iterate over each cache piece
			for p in self.PieceCache:
				if p == path:
					return True
			return False


	def SendFile(self, remotepath, localpath):
		if not os.path.isfile(localpath):
			print "ERROR: crepositoryserver::SendFile() does not exist: " + localpath
			return -1

		SendFileToServer(self.get_host(), self.get_user(), remotepath, localpath)

		# todo: update DfCache here, something like:
		# self.DfConsumeFile(localpath)

		# update free space info
		self.Df()

		return 0


	def ReceiveFile(self, remotepath, localpath):
		if os.path.isfile(localpath):
			print "WARNING: crepositoryserver::ReceiveFile() overwriting existing file: " + localpath

		GetFileFromServer(self.get_host(), self.get_user(), remotepath, localpath)
		# todo: update DfCache here

		return 0

	def RemoveFile(self, remotepath):
		RemoveFileFromServer(self.get_host(), self.get_user(), remotepath)

		# todo: update DfCache here, something like:
		# self.DfFreeFile(localpath)

		# update free space info
		self.Df()

		return 0