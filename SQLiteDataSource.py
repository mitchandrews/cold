#!/usr/bin/python
#
# Mitch Andrews

# dependencies:
#  global imports (sys, os, et al)
#
#  cfroutines
#  SQLite


## Functionality that might be missing:
#	* 
#
#
## SQLite table structure:
#
# FileList.Type		:=			( 0 -> file, 1 -> dir )
#

import sys
import os
import subprocess
import re
import sqlite3
from ccfroutines import *


class SQLiteDataSource:

	# Member Variables:
	#
	# String dbPath
	# SQLiteConnection Con
	# SQLiteConnCursor Cur

	def __init__(self):
		print " ### SQLiteDataSource Init"
		self.dbPath = "cold.db"
		
		# Create db if necessary
		if not os.path.isfile(self.dbPath):
			print " ## DB file not found! creating:", self.dbPath
			self.initDb(self.dbPath)
		else:
			# Connect to db
			self.Con = sqlite3.connect(self.dbPath)
			self.Cur = self.Con.cursor()
			
			
		
	def initDb(self, path):
	
		# Connect to db
		self.Con = sqlite3.connect(self.dbPath)
		self.Cur = self.Con.cursor()
		
		print " ## initDb QUERY: DROP TABLE IF EXISTS PieceList"
		self.Cur.execute("DROP TABLE IF EXISTS PieceList")
		print " ## initDb QUERY: DROP TABLE IF EXISTS FileList"
		self.Cur.execute("DROP TABLE IF EXISTS FileList")
		self.Con.commit()
	
		print " ## initDb QUERY: CREATE TABLE PieceList(Id INTEGER PRIMARY KEY, SequenceNo INT, Hash TEXT, FileID INT)"
		self.Cur.execute("CREATE TABLE PieceList(Id INTEGER PRIMARY KEY, SequenceNo INT, Hash TEXT, FileID INT)")
		print " ## initDb QUERY: CREATE TABLE FileList(Id INTEGER PRIMARY KEY, Type INT, Name TEXT, ParentId INT)"
		self.Cur.execute("CREATE TABLE FileList(Id INTEGER PRIMARY KEY, Type INT, Name TEXT, ParentId INT)")
		
		# Create root folder
		print " ## initDb QUERY: INSERT INTO FileList values(0, 1, '/', 0)"
		self.Cur.execute("INSERT INTO FileList values(0, 1, '/', 0)")
		
		self.Con.commit()
		
		
	#def copyFile(self, fromPath, toPath):
	#	pass
		
		
	def createFile(self, path, pieceList=[]):
		print ' ## SQLiteDataSource createFile', path, len(pieceList)
		
		(dirname, fname) = os.path.split(os.path.normpath(path))
		
		# dirList = os.path.normpath(path).split('/')
		# dirList = filter(None, dirList)
		# fname = dirList.pop()
		# dirname = '/'
		# for d in dirList:
			# dirname = dirname + d + '/'
		
		print ' ## SQLiteDataSource createFile (fname, dirname) :=', fname, '::', dirname
		
		# Always create all required subdirectories automatically
		dirId = self.mkdirs(dirname)
		
		# Check if it already exists and is a file
		(id, type) = self.getId(path)
		if type == 0 and id > -1:
		
			# If so, remove
			self.remove(path)
			
		# Create new
		print " ## createFile QUERY: INSERT INTO FileList(Type, Name, ParentId) values(0, '%s', %d) ##" % (fname, dirId)
		self.Cur.execute("INSERT INTO FileList(Type, Name, ParentId) values(0, '%s', %d)" % (fname, dirId))
		self.Con.commit()
		
		# Get our new FileId
		(id, type) = self.getId(path)
		
		# Write pieces associated with file
		i = 0
		for p in pieceList:
			print " ## createFile QUERY: INSERT INTO PieceList(SequenceNo, Hash, FileId) values(%d, '%s', %d)" % (i, p, id)
			self.Cur.execute("INSERT INTO PieceList(SequenceNo, Hash, FileId) values(%d, '%s', %d)" % (i, p, id))
			i = i + 1
		
		self.Con.commit()
		return
		
		
	#def findFile(self, fname):
	#	return "/" + fname
	
	
	# RETURNS: tuple: (id, type)	type := <0|1> : 0 -> file, 1 -> dir
	def getId(self, path):
		print ' ## SQLiteDataSource getId', path
		
		dirList = os.path.normpath(path).split('/')
		dirList = filter(None, dirList)
		objId = 0
		objType = 1		# 0 => file, 1 => dir
		
		# Iterate of path, finding Id of each subfolder
		while len(dirList) > 0:
			if len(dirList[0].strip()) == 0:
				del dirList[0]
				continue
			pathElem = dirList[0]
			del dirList[0]
			
			# Get ID, Type of next path element

			print " ## getId QUERY: SELECT Id, Type from FileList WHERE Name = '%s' and ParentId = %d" % (pathElem, objId)
			r = self.Cur.execute("SELECT Id, Type from FileList WHERE Name = '%s' and ParentId = %d" % (pathElem, objId))
			self.Con.commit()
			t = r.fetchone()
			if t is not None:
				objId = t[0]
				objType = t[1]
				
				if objType == 0:
					print "found file!:", pathElem
				elif objType == 1:
					print "found dir!:", pathElem
				continue
					
			# print "not found!:", pathElem
			return (-1, -1)
				
		#if len(path) == 0:
		#	fname = "/"
			
		print ' ## SQLiteDataSource getId returning:', objId, objType
		return (objId, objType)

		
		
	def listFilePieces(self, path):
		print " ## SQLiteDataSource listFilePieces", path
		
		(id, type) = self.getId(path)
		
		# if not regular file, return error
		if type != 0:
			return []
			
		print " ## listFilePieces QUERY: SELECT Hash from PieceList WHERE FileId = %d ORDER BY SequenceNo ASC" % id
		r = self.Cur.execute("SELECT Hash from PieceList WHERE FileId = %d ORDER BY SequenceNo ASC" % id)
		self.Con.commit()
		
		# Extract r.fetchall()[0] to new list
		t = []
		for i in r.fetchall():
			t.append(i[0])
			
		return t
		
		
	def ls(self, path):
		print " ## SQLiteDataSource ls", path
		
		(id, type) = self.getId(path)
		dirList = os.path.normpath(path).split('/')
		dirList = filter(None, dirList)
		
		print " ## ls (id, type):", id, type

		# if directory, return list of contents
		if type == 1:
			
			print " ## ls QUERY: SELECT Name from FileList WHERE ParentId = %d" % (id)
			r = self.Cur.execute("SELECT Name from FileList WHERE ParentId = %d" % (id))
			self.Con.commit()
			fileList = []
			print "ls results:"
			for t in r.fetchall():
				print "got:", t[0]
				fileList.append(t[0])
		
			return fileList
			
		# if file, return only self
		elif type == 0:
			return [dirList[-1]]
			
		else:
			print " ## ls Error: not found!", path
			return []
		
		
		
	#def mkdir(self, path):
	#	pass
		
		
	# Make directories recursively to satisfy 'path'
	def mkdirs(self, path):
		print " ## SQLiteDataSource mkdirs", path
		dir_i = 0
		dirList = os.path.normpath(path).split('/')
		dirList = filter(None, dirList)
		
		print " ## mkdirs dirList:", dirList
		
		# Iterate of path, finding Id of each subfolder
		while len(dirList) > 0:
			if len(dirList[0].strip()) == 0:
				del dirList[0]
				continue
			thisDir = dirList[0]
			del dirList[0]
			
			# If file already exists, get its Id
			print " ## mkdirs QUERY: SELECT Id, Type from FileList WHERE Name = '%s' and ParentId = %d and Type = 1 LIMIT 1" % (thisDir, dir_i)
			r = self.Cur.execute("SELECT Id, Type from FileList WHERE Name = '%s' and ParentId = %d and Type = 1 LIMIT 1" % (thisDir, dir_i))
			self.Con.commit()
			t = r.fetchone()
			print "returned rows:", t
			
			if t is None:
				# if 'thisDir' doesn't exist, create it

				print " ## mkdirs QUERY: INSERT INTO FileList(Type, Name, ParentId) VALUES(1, '%s', %d) ##" % (thisDir, dir_i)
				self.Cur.execute("INSERT INTO FileList(Type, Name, ParentId) VALUES(1, '%s', %d)" % (thisDir, dir_i))
				self.Con.commit()
				# get newly created folder's id
				print " ## mkdirs QUERY: SELECT Id from FileList WHERE Name = '%s' and ParentId = %d and Type = 1" % (thisDir, dir_i)
				r = self.Cur.execute("SELECT Id from FileList WHERE Name = '%s' and ParentId = %d and Type = 1" % (thisDir, dir_i))
				self.Con.commit()
				t = r.fetchone()
				dir_i = t[0]
					
				continue
			
			if t[1] == 0:
				print " ## mkdirs Error: path contains existing regular file!"
				return -1
			
			# Get ID of next dir in path
			dir_i = t[0]
		
		return dir_i
		
		
	#def mv(self, fromPath, toPath):
	#	pass
		
		
	# * CREDIT DUE: 
	#	 http://stackoverflow.com/questions/764710/sqlite-performance-benchmark-why-is-memory-so-slow-only-1-5x-as-fast-as-d
	def querytime(conn,query):
		start = time.time()
		foo = conn.execute(query).fetchall()
		diff = time.time() - start
		return diff
		

	# Recursively calls itself on directories
	# Recursion terminates on files
	# 
	# requires: self.ls(), self.listFilePieces(), self.getId()
	#
	def remove(self, path):
		print " ## SQLiteDataSource remove", path
		
		
		id, type = self.getId(path)
		print " (", id, ",", type, ")"
	
		# if 'path' is a directory, recursively call self.remove() on contents
		if type == 1:
			subfiles = self.ls(path)
			for f in subfiles:
				print "f:", f
				self.remove(path + "/" + f)
			
			print " ## remove QUERY: DELETE FROM FileList WHERE Id = %s" % id
			self.Cur.execute("DELETE FROM FileList WHERE Id = %s" % id)
			self.Con.commit()
			return 0
		
		# if 'path' is a file, delete pieces and terminate
		elif type == 0:
			pieces = self.listFilePieces(path)
			
			# Delete pieces
			print " ## remove QUERY: DELETE FROM PieceList WHERE FileId = %s" % id
			self.Cur.execute("DELETE FROM PieceList WHERE FileId = %s" % id)
			
			# Delete the file
			print " ## remove QUERY: DELETE FROM FileList WHERE Id = %s" % id
			self.Cur.execute("DELETE FROM FileList WHERE Id = %s" % id)
			
			self.Con.commit()
			return 0
			
		else:
			return -1
			
		return -1
		
		
	def truncateFile(self, path):
		print " ## SQLiteDataSource truncateFile", path
		
		(id, type) = self.getId(path)
		
		# if dir, skip
		if type == 1:
			return 0
		
		# if file: delete, create
		elif type == 0:
			self.remove(self.path)
			self.createFile(self.path)
		
		return -1
	
