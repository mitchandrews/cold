#!/bin/bash

if [[ "$1" == "-r" ]]
then
	USER=`whoami`
	if [[ "$USER" != "root" ]]
	then
		rm -rf /home/$USER/.cache/*
		rm -rf /home/$USER/maps/*
		rm -rf /home/$USER/receive/*
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
rm -f coldd.py*
rm -f cglobals.py*
rm -f cold.py*
rm -f crepositoryserver.py*
rm -f SQLiteDataSource.py*
rm -f refresh.sh*
rm -f filter-sha1-inbounds.sh*
rm -f filter-sha1-outbounds.sh*
rm -f mv-pieces-inbounds.sh*
rm -f rm-pieces-inbounds.sh*
rm -f hashgen.sh*
rm -f options.txt*
rm -f layout.txt*
rm -f layout-1.txt*
rm -f layout-2.txt*
rm -f cold.db*
rm -f output.txt* 2>/dev/null
rm -f monitor.sh*
rm -f id_rsa_cold*

sudo rm /tmp/.cold* /tmp/filter-sha1* /tmp/rm-pieces* /tmp/mv-pieces* 2>/dev/null

wget -q http://192.168.0.106:8080/repo/ccfroutines.py
wget -q http://192.168.0.106:8080/repo/ccold.py
wget -q http://192.168.0.106:8080/repo/coldd.py
wget -q http://192.168.0.106:8080/repo/cglobals.py
wget -q http://192.168.0.106:8080/repo/cold.py
wget -q http://192.168.0.106:8080/repo/crepositoryserver.py
wget -q http://192.168.0.106:8080/repo/SQLiteDataSource.py
wget -q http://192.168.0.106:8080/repo/refresh.sh
wget -q http://192.168.0.106:8080/repo/filter-sha1-inbounds.sh
wget -q http://192.168.0.106:8080/repo/filter-sha1-outbounds.sh
wget -q http://192.168.0.106:8080/repo/mv-pieces-inbounds.sh
wget -q http://192.168.0.106:8080/repo/rm-pieces-inbounds.sh
wget -q http://192.168.0.106:8080/repo/hashgen.sh
wget -q http://192.168.0.106:8080/repo/options.txt
wget -q http://192.168.0.106:8080/repo/layout.txt
wget -q http://192.168.0.106:8080/repo/layout-1.txt
wget -q http://192.168.0.106:8080/repo/layout-2.txt
wget -q http://192.168.0.106:8080/repo/cold.db
wget -q http://192.168.0.106:8080/repo/monitor.sh
wget -q http://192.168.0.106:8080/repo/id_rsa_cold
wget -q http://192.168.0.106:8080/repo/id_rsa_cold.pub


chmod 755 *.sh 2>/dev/null
