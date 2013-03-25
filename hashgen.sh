#!/bin/bash

i=0
while [[ $i -lt 10000 ]]
do
	echo $(( $RANDOM * $RANDOM )) >/tmp/hashgen.txt
	hash=`sha1sum /tmp/hashgen.txt | awk '{print $1}'`
	echo "$hash" >>/tmp/hashhive.txt
	(( i++ ))
done

rm /tmp/hashgen.txt
exit 0
