#!/bin/bash

i = 0

while [[ $i -lt 20 ]]
do
	echo "doing run $i..."
	sudo bash refresh.sh -r ; bash refresh.sh ; clear ; python cold.py -s file1.raw >/dev/null 2>/dev/null
	ls -lR /home/cold*/repository | wc -l >>consistency.txt
	(( i++ ))
done
