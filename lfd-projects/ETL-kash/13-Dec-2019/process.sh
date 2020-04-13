#!/bin/bash
before=$(ls ../raw_synced_data/ | wc -l)
convertsecs() {
 ((h=${1}/3600))
 ((m=(${1}%3600)/60))
 ((s=${1}%60))
 printf "%02d:%02d:%02d\n" $h $m $s
}

echo "Start at `date`"
echo "Syncing Updated Data from Remote Server"
rsync -zarv lfdkash@193.42.121.110:/home/lfdkash/raw_files/ /home/amir/raw_synced_data/

after=$(ls ../raw_synced_data/ | wc -l)
COUNT=`expr $after - $before`
echo $COUNT Files updated


echo "Sync Complete at `date`"
#Process data
echo "Processing Raw Data"
start=`date +%s`
python3 /home/amir/Script/Main-script.py
end=`date +%s`
runtime=$((end-start))
echo $runtime
echo "Complete at `date` in `convertsecs $runtime`"

