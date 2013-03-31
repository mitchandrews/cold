#!/bin/bash

# args:
#	$1:		target server (form: user@host:path)
#	$2:		local repository path

if [[ "$1" == "" ]]
then
	exit 1
fi
if [[ "$2" == "" ]]
then
	exit 2
fi

targetServer="$1"
#touch "/tmp/.cold.`whoami`.inbounds.txt"

while read line
do
	scp -p "$2/$line" "$1/"
done
exit 0
