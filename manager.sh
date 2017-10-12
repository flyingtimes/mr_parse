#!/bin/bash
updateProgram ()
{
	for i in $(cat hosts.conf)
        do
		echo "< updating program to $i >"
                scp *.sh *.py *.ini *.conf $i:/home/enoss/mr_parse/
		scp libs/*.py $i:/home/enoss/mr_parse/libs
        done
}
stopService ()
{       
        for i in $(cat hosts.conf)
        do      
                rs=`ssh $i "cd mr_parse;supervisorctl stop client"`
		echo "< $i says: $rs >"
        done
	supervisorctl stop master
}
checkAlive ()
{
	for i in $(cat hosts.conf)
	do
		rs=`ssh $i "ps -ef|grep client.py|wc -l"`
		echo "< $[rs-2] thread alive in $i >"
		echo "deamon status:"
		sts=`ssh $i "cd mr_parse;supervisorctl status"`
		echo "$sts"
	done
	ssh sscloud11 "cd mr-storm;bash list_rabbitmq_queue.sh"
}
startService ()
{
	for i in $(cat hosts.conf)
	do
		DIR=/mem_swap
		if [ "$(ls -A $DIR)" ]; then
		    ssh $i "rm /mem_swap/*"
		fi
		ssh $i "cd mr_parse;supervisorctl start client"
	done
	cd home/enoss/mr_parse;supervisorctl start master
}
restartService ()
{
  stopService
  stopService
  startService
}
case $1 in
  start  ) startService   ;;
  update ) updateProgram  ;;
  check  ) checkAlive     ;;
  stop   ) stopService    ;;
  restart) restartService ;;
  *      ) echo "$0: unknown argument: $1";;
esac

