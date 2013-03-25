#!/bin/bash

if [[ "$1" == "-c" ]]
then
	rm "/tmp/tablet-*" 2>/dev/null
	exit 0
fi

CMD_FILE="/tmp/tablet-exec.cmd"
PID_FILE="/tmp/tablet-exec.pid"
CHILDPID_FILE="/tmp/tablet-child.pid"
REFRESH_FILE="/tmp/tablet-exec.refresh"
OLD_PID=`cat "$PID_FILE" 2>/dev/null`
OLDCHILD_PID=`cat "$CHILDPID_FILE" 2>/dev/null`
CMD="w"

if [[ "$1" != "" ]]
then
	CMD="$@"
fi

# if already running, send usr1
if [[ "$OLD_PID" != "" && "`ps ax | grep $OLD_PID | grep -v grep`" != "" ]]
then
	echo "$CMD" >$CMD_FILE
	kill -s SIGUSR1 $OLD_PID 2>/dev/null
	exit 0
fi

echo "$$" >"$PID_FILE"

clear

# spawn proc
echo "$CMD" | /bin/bash &
echo "$!" >$CHILDPID_FILE
echo "$$" >$PID_FILE

Refr()
{
	OLD_PID=`cat "$PID_FILE"`
	CMD=`cat "$CMD_FILE"`
	
	if [[ "$CMD" == "" ]]
	then
		trap 'Refr' SIGUSR1
		return
	fi	
	
	kill -9 $OLDCHILD_PID 2>/dev/null
	
	touch "$REFRESH_FILE"
	clear
	# spawn proc
	echo "$CMD" | /bin/bash &
	CHILD_PID=$!
	echo "$CHILD_PID" >$CHILDPID_FILE
	
	echo "" >"$CMD_FILE"
	
	trap 'Refr' SIGUSR1
}
trap 'Refr' SIGUSR1

while [[ 1 ]]
do
	sleep 1
	Refr
done
