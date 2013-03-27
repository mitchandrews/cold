#!/bin/bash
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

rm -f "/tmp/.cold.`whoami`.inbounds.txt"
touch "/tmp/.cold.`whoami`.inbounds.txt"

while read line
do
	if [ "$line" \> "$1" ]
	then
		if [ "$line" \< "$2" ]
		then
			echo $line >>"/tmp/.cold.`whoami`.inbounds.txt"
		fi
	fi
done
exit 0
