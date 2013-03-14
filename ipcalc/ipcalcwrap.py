import sys
import os
import subprocess
import re
import ipcalc

def ShellOutputLines(cmd):

	p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
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

	ret = subprocess.call("ping -qo -t 1 -c 3 %s" % ip, shell=True, stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)
	if ret == 0:
		print "%s: is alive" % ip
		return True
	else:
		print "%s: did not respond" % ip
		return False
	
# loop through each argument string	
for s in sys.argv[1:]:
	ret = ipcalc.Network(s)
	for r in ret:
		print r
		
		