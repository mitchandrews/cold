#!/usr/bin/python
#
# Mitch Andrews
# 11/15/10

# cold.cfroutines.py program dependencies:
#  global imports (sys, os, et al)
#
#  -- requirements should be kept to almost zero,
#   so this can be the first include in the program.

import imp
import os
import pipes
import random
import re
import string
import subprocess
import sys
import time
# req for ipcalc.Network():
ipcalc_mod = imp.load_source("ipcalc", "./ipcalc/ipcalc.py")

## Still probably need:
#	mkdir_remote(host, user, remotepath)
#	remove_motd(host, user)
#	is_dir_remote(host, user, remotepath)
#	file_size(host, user, remotepath)
#	sshd_log_level_error()	#for local host only


# This function executes 'cmd' in the shell as a literal, splits the text by End-of-Line,
#  deletes whitespace-only indicies, and returns the remaining lines as a list
def ShellOutputLines(cmd):

	p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
	#p.wait()
	temp = p.stdout.read()
	temp = temp.split('\n')

	returnlist = []

	# check the lines for only-whitespace entries
	index = 0
	for j in temp:
		if (j.isspace() or len(j) == 0):
			pass
		else:
			returnlist.append(j)
	return returnlist


def WDir():
	return os.path.abspath('.')
#	return ShellOutputLines('pwd').pop()	#also works



# SSH routines

# serverls returns an array of the contents of a remote directory using ssh.
# the connection is assumed to be passwordless, and this function is
# otherwise undefined.
def serverls(host, user, remotepath):

	if (host == "" or user == "" or remotepath == ""):
		print "serverls error"
		return "-1"

#	output = subprocess.check_output(
#		['ssh', user + '@' + host, 'ls', remotepath],
#		stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

	p = subprocess.Popen(['ssh', '-T', '-q', user + '@' + host], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	#p.wait()

	output,err = p.communicate(r'ls "%s"' % remotepath)

	output = output.split('\n')

	return (output, err.strip())


# serverdf returns the number of kilobytes available on the device
#  hosting the file <remotepath>.
# returns:
#	int >= 0	number of free kilobytes
#	-1			error
def serverdf(host, user, remotepath):

	if (host == "" or user == "" or remotepath == ""):
		print "serverdf error"
		return "-1"

	# df -B 1024 . | tail -n +2 | awk '{print $4}'
	p = subprocess.Popen(['ssh', '-T', '-q', user + '@' + host], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	#p.wait()
	
	output,err = p.communicate("df -B 1024 . | tail -n +2 | awk '{print $4}' \"%s\"" % remotepath)
#	p.terminate()

	if (len(output) == 0 or int(output) < 0):
		print "serverdf result error"
		return "-1"

	return output.strip()


def IsFile(host, user, remotepath):
	#p = subprocess.Popen(['ssh', host, 'test -f %s' % pipes.quote(remotepath)])
	p = subprocess.Popen(['ssh', '-T', '-q', user + '@' + host, 'test -f ' + pipes.quote(remotepath)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	p.wait()
	#print " # IsFile() return code:", pipes.quote(remotepath), (p.returncode == 0)
	
	return (p.returncode == 0)


# Called by SendFileToCloud(path) to send pieces from a map
# Called indirectly by -s [--send] flag
def SendFileToServer(host, user, remotepath, localpath):

	scpstring = user + "@" + host + ":" + remotepath

	#print "scp: " + localpath.strip() + " " + scpstring.strip()

	p = subprocess.Popen(['scp', localpath, scpstring], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	#p.wait()
#	p.terminate()

# Called by ReceiveFromCloud() to receive pieces from a map
# Called indirectly by -r [--receive] flag
# RETURNS:
#	zero:		no error
#	non-zero:	error
def GetFileFromServer(host, user, remotepath, localpath):

	# if file already exists and is not a file (e.g. directory, device): (error)
	if os.path.exists(localpath) and not os.path.isfile(localpath):
		print "ERROR: GFFS(): path exists as non-file: " + localpath
		return -4

	# if file already exists: (error)
	i=20
	while os.path.isfile(localpath) and i > 0:
		print "sleeping (exists)..."
		time.sleep(.1)
		i=i-1
	# if file still exists, return an error
	if os.path.isfile(localpath):
		print "ERROR: GFFS(): could not override file: " + localpath
		return -3

	scpstring = user + "@" + host + ":" + remotepath

#	print "scp: " + scpstring.strip() + " " + localpath.strip()

	i = 0
	while not os.path.isfile(localpath):
		p = subprocess.Popen(['scp', scpstring, localpath], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		p.wait()
		time.sleep(random.random() / 10)
		i = i + 1
		
		if i > 4:
			print " ## Error max retries: GetFileFromServer", host, user, remotepath, localpath
			return -2

	# i=20
	# while not os.path.exists(localpath) and i > 0:
		# print "sleeping (exists)..."
		# time.sleep(.1)
		# i=i-1
	# if not os.path.exists(localpath):
		# return -1

	# j=20
	# while not os.path.getsize(localpath) and j > 0:
		# print "sleeping (size)..."
		# time.sleep(.1)
		# j=j-1
	
	if os.path.getsize(localpath) < 1:
		return -2

	# return zero for no error
	return 0


# PURPOSE: Remove a file at 'remotepath' from host over ssh
def RemoveFileFromServer(host, user, remotepath):
	p = subprocess.Popen(['ssh', '-T', user + '@' + host, "rm " + remotepath])
	#p.wait()

	# return zero for no error
	return 0


# PURPOSE: return the size of a remote file in KB
#def GetRemoteFileSize(host, user, remotepath):
#	p = subprocess.Popen(['ssh', '-T', user + '@' + host, "df -h " + remotepath])


# Check the connection to user@host, return true if passwordless logins are enabled, false otherwise
def HasPasswordlessSSH(host, user):

	# The command to check passworded/passwordless:
	#  $ ssh -o 'PreferredAuthentications=publickey' cold@ppgbox "echo"
	#  which prints "Permission denied (publickey,keyboard-interactive)." on failure, empty on success
#	p = subprocess.Popen(['ssh', '-o \'PreferredAuthentications=publickey\'', user + '@' + host, 'echo'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
	ret = subprocess.call(['ssh', '-q', '-T', '-o PreferredAuthentications=publickey', '-o UserKnownHostsFile=/dev/null', '-o StrictHostKeyChecking=no', user + '@' + host, 'echo'], stdout=subprocess.PIPE)
#	print "ret: %i" % ret

	if ret == 0:
		return True
	else:
		return False


# assumes a password-less login
def MuteSSHLogin(host, user):
	# Verify passwordless
	if subprocess.call(['ssh', '-T', '-o PreferredAuthentications=publickey', '-o UserKnownHostsFile=/dev/null', '-o StrictHostKeyChecking=no', user + '@' + host, 'echo'], stdout=subprocess.PIPE) == 0:
		p = subprocess.Popen(['ssh', '-T', user + '@' + host, 'touch .hushlogin'])
		#p.wait()


# Given a string, determine if it is a valid MD5 hash
# Returns:
#	True/False
def IsValidMD5(md5string):

	md5string = md5string.strip()

	# check character validity
	if re.match(r'^[a-zA-Z0-9]{32}$', md5string) is not None:
		return True

	# check length
	if len(md5string) != 32:
		return False

	return False
	
# Given a string, determine if it is a valid SHA-1 hash
# Returns:
#	True/False
def IsValidSHA1(sha1string):

	sha1string = sha1string.strip()

	# check character validity
	if re.match(r'^[a-zA-Z0-9]{40}$', sha1string) is not None:
		return True

	# check length
	if len(sha1string) != 32:
		return False

	return False


# PURPOSE:	Given a subnet string in the format: 192.168.0.0/24
#			return the list of IPs constituting the entire subnet
# NOTES:	The maximum return size is capped at 66,000, which can accommodate
#			a class B address (256^2)
#			All IPs ending in .0 and .255 are stripped before returning, so
#			only pingable IPs are returned
def ListSubnetIPs(subnet='10.0.1.0/24'):

	# return variable
	iplist = []

	for x in ipcalc_mod.Network(subnet):
		lsatipbyte=string.rsplit(str(x), '.', 1).pop()
#		print str(x) + " : " + lsatipbyte

		# if IP does not end in 0 or 255, add it to return list
		if not (lsatipbyte == "0" or lsatipbyte == "255"):
			iplist.append(str(x))

	return iplist


# PURPOSE:	Given an IP, ping it and return status
# RETURNS:
#	Boolean		whether or not IP is pingable
def IsHostAlive(ip, maxtries=3):

	ret = subprocess.call("ping -q -t 2 -c 3 %s" % ip, shell=True, stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)
	if ret == 0:
		print "%s: is alive" % ip
		return True
	else:
		print "%s: did not respond" % ip
		return False

