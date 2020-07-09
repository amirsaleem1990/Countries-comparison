#!/bin/bash

function geT {
	echo -n "$1:  " >> counts.txt
	c=`mysql -u db_user -p'passw0rd'  -D kashat -e "select count(*) from $1 ;" 2>/dev/null | tail -1`
	echo $c >> counts.txt
	#echo "$c : $1"
}
echo -n "" >  counts.txt


geT accounts_list
geT app_install_log
geT calendar_events
geT call_log
geT contacts_list
geT ext_storage_files
geT filter_app_log
geT gallery_data
geT location
geT master
geT outgoing_call_log
geT sms_log
geT sms_sent_log

cat counts.txt

total=0
while IFS= read -r  line ;
	do total=$(($total + `echo "$line" | cut -d: -f2 | sed 's/^ //g'`))
done < counts.txt
echo -e "\n\nTotal: $total\n\n"


del -rf counts.txt
