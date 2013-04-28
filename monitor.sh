#!/bin/bash

watch 'sar -n DEV 1 1; echo ; df -ha | grep repo ; echo
cat layout.txt | grep cold1 ; echo -n `ls /home/cold1/repository/ | head -n 1`; echo -n "  " ; ls /home/cold1/repository/ | tail -n 1; echo
cat layout.txt | grep cold2 ; echo -n `ls /home/cold2/repository/ | head -n 1`; echo -n "  " ; ls /home/cold2/repository/ | tail -n 1; echo
cat layout.txt | grep cold3 ; echo -n `ls /home/cold3/repository/ | head -n 1`; echo -n "  " ; ls /home/cold3/repository/ | tail -n 1; echo
cat layout.txt | grep cold4 ; echo -n `ls /home/cold4/repository/ | head -n 1`; echo -n "  " ; ls /home/cold4/repository/ | tail -n 1; echo
cat layout.txt | grep cold5 ; echo -n `ls /home/cold5/repository/ | head -n 1`; echo -n "  " ; ls /home/cold5/repository/ | tail -n 1; echo
cat layout.txt | grep cold6 ; echo -n `ls /home/cold6/repository/ | head -n 1`; echo -n "  " ; ls /home/cold6/repository/ | tail -n 1;'

exit
