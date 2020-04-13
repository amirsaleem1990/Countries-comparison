convertsecs() {
 ((h=${1}/3600))
 ((m=(${1}%3600)/60))
 ((s=${1}%60))
 printf "%02d:%02d:%02d\n" $h $m $s
}


start=`date +%s`
python3 /home/amir/github/LFD-projects/ETL-kash/13-Dec-2019/Script/Main-script.py
end=`date +%s`
runtime=$((end-start))
echo $runtime
echo "Complete at `date` in `convertsecs $runtime`"