#!/bin/bash
# rsync -au
# rsync -zarv

echo -e "***********************************************************************************\nStart: syncing\n"

function foo () {
	ls /home/amir/raw_synced_data | wc -l
}

function fo () {
	ls -l /home/amir/ --time-style=long-iso | grep  raw_synced_data | sed 's/\ \{1,\}/,/g' | cut -d "," -f6,7 | sed 's/,/\ | /g'
}
before_time=`fo`

convertsecs() {
 ((h=${1}/3600))
 ((m=(${1}%3600)/60))
 ((s=${1}%60))
 printf "%02d:%02d:%02d\n" $h $m $s
}

b=`foo`

start=`date +%s`
timeout=$1
if [[ $timeout = "" ]] ; then
	read -p "Enter timeout seconds [default 120]: " timeout
		if [[ $timeout = "" ]] ; then
			timeout=120
		fi
fi

tries=$2
if [[ $tries = "" ]] ; then
	read -p "Enter tries Qty [default 5]: " tries
	if [[ $tries = "" ]] ; then
		tries=5
	fi
fi


t=1
synced=False

trap "exit" INT
while [ $t -le $tries ]; do
	t=$(( $t + 1 ))
	rsync --timeout=$timeout -zar lfdkash@193.42.121.110:/home/lfdkash/raw_files/ /home/amir/raw_synced_data/ 2>/dev/null
	if [[ $? = 0 ]]; then
		# echo -e "\nSuccessfully synced data `date +%H:%M:%S`"
		synced=True
		break
	else
		echo "rsyn Error, we'll try again `date +%H:%M:%S`"
	fi
done

if [[ $synced = "False" ]] ; then
	echo -e "\n\nSorry, We got an error in each our attempts to sync the data\n"
	exit
fi

end=`date +%s`
runtime=$((end-start))

a=`foo`

after_time=`fo`

diff=$(( $a - $b ))
if [[ $diff > 0 ]]; then
	echo "Before: $b Files"
	echo "After : $a Files"
	echo "Added : $diff files"
	echo -e "\nLast update time BEFORE this attempt : $before_time"
	echo "Last update time AFTER  this attempt : $after_time"
	echo -e "\n\nSuccessfully synced in `convertsecs $runtime`"
else
	echo "Sorry, no more data"
fi

echo -e "***********************************************************************************"

