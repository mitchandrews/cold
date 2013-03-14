#!/bin/bash

if [[ "$1" == "-r" ]]
then
	USER=`whoami`
	if [[ "$USER" != "root" ]]
	then
		rm -rf /home/$USER/.cache/*
		rm -rf /home/$USER/maps/*
	fi

	c=7
	i=1
	while [ $i -lt $c ]
	do
		# clear repositories
		rm -rf /home/cold$i/repository/*
		(( i++ ))
		
	done
fi

rm -f ccfroutines.py*
rm -f ccold.py*
rm -f cglobals.py*
rm -f cold.py*
rm -f crepositoryserver.py*
rm -f refresh.sh*
rm -f options.txt*

wget -q http://192.168.0.106:8080/www/ccfroutines.py
wget -q http://192.168.0.106:8080/www/ccold.py
wget -q http://192.168.0.106:8080/www/cglobals.py
wget -q http://192.168.0.106:8080/www/cold.py
wget -q http://192.168.0.106:8080/www/crepositoryserver.py

wget -q http://192.168.0.106:8080/www/refresh.sh
wget -q http://192.168.0.106:8080/www/options.txt

chmod 755 *.sh
