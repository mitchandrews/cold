#!/bin/bash

# args:
#	$1:		lower bound
#	$2:		upper bound
#	$3:		local repository path

# NOTES:
#
#	* lines fed to stdin from "/tmp/.cold.`whoami`.inbounds.txt"

if [[ "$1" == "" ]]
then
	exit 1
fi
if [[ "$2" == "" ]]
then
	exit 2
fi
if [[ "$3" == "" ]]
then
	exit 2
fi

lo="`echo $1 | head -c 40`"
hi="`echo $2 | head -c 40`"

if [[ "$lo" == "0" ]]
then
	lo="0000000000000000000000000000000000000000"
fi
if [[ "$hi" == "0" ]]
then
	hi="0000000000000000000000000000000000000000"
fi

while read line
do
	rm -f "$3/$line"
done
exit 0
