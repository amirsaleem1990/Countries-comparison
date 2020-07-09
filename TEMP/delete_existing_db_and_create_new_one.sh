#!/bin/bash


mysql -u db_user -p'passw0rd' -D kashat -e "delete from accounts_list;"
mysql -u db_user -p'passw0rd' -D kashat -e "delete from calendar_events;"
mysql -u db_user -p'passw0rd' -D kashat -e "delete from sms_log;"
mysql -u db_user -p'passw0rd' -D kashat -e "delete from call_log;"
mysql -u db_user -p'passw0rd' -D kashat -e "delete from contacts_list;"
mysql -u db_user -p'passw0rd' -D kashat -e "delete from app_install_log;"
mysql -u db_user -p'passw0rd' -D kashat -e "delete from filter_app_log;"
mysql -u db_user -p'passw0rd' -D kashat -e "delete from master;"
mysql -u db_user -p'passw0rd' -D kashat -e "delete from ext_storage_files;"
mysql -u db_user -p'passw0rd' -D kashat -e "delete from location;"
mysql -u db_user -p'passw0rd' -D kashat -e "delete from outgoing_call_log;"
mysql -u db_user -p'passw0rd' -D kashat -e "delete from sms_sent_log;"
mysql -u db_user -p'passw0rd' -D kashat -e "delete from gallery_data;"

mysql -u db_user -p'passw0rd' -D kashat -e "SET GLOBAL max_allowed_packet=512000000;"
#----------------------------------------------------------------------------------------------------------------------
# create new user
# CREATE USER 'db_user'@'localhost' IDENTIFIED BY 'passw0rd';
# GRANT ALL PRIVILEGES ON *.* TO 'db_user' @'localhost' WITH GRANT OPTION;
# FLUSH PRIVILEGES;

# Drop existing database
# mysql -u db_user -p'passw0rd'  -e "DROP DATABASE kashat;" 2>/dev/null
									

# Crete new database
# mysql -u db_user -p'passw0rd' -e "CREATE DATABASE kashat CHARACTER SET utf8mb4;"


# python3 initialize_DB_file_if_not_exist.py



# mysql -u db_user -p'passw0rd' -D kashat -e "CREATE INDEX Ind ON accounts_list     (end_date(240))"
# mysql -u db_user -p'passw0rd' -D kashat -e "CREATE INDEX Ind ON calendar_events   (end_date(240))"
# mysql -u db_user -p'passw0rd' -D kashat -e "CREATE INDEX Ind ON sms_log           (end_date(240))"
# mysql -u db_user -p'passw0rd' -D kashat -e "CREATE INDEX Ind ON call_log          (end_date(240))"
# mysql -u db_user -p'passw0rd' -D kashat -e "CREATE INDEX Ind ON contacts_list     (end_date(240))"
# mysql -u db_user -p'passw0rd' -D kashat -e "CREATE INDEX Ind ON app_install_log   (end_date(240))"
# mysql -u db_user -p'passw0rd' -D kashat -e "CREATE INDEX Ind ON filter_app_log    (end_date(240))"
# mysql -u db_user -p'passw0rd' -D kashat -e "CREATE INDEX Ind ON master            (end_date(240))"
# mysql -u db_user -p'passw0rd' -D kashat -e "CREATE INDEX Ind ON ext_storage_files (end_date(240))"
# mysql -u db_user -p'passw0rd' -D kashat -e "CREATE INDEX Ind ON gallery_data      (end_date(240))"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE accounts_list     ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE calendar_events   ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE sms_log           ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE call_log          ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE contacts_list     ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE app_install_log   ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE filter_app_log    ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE master            ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE ext_storage_files ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE location          ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE outgoing_call_log ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE sms_sent_log      ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
# mysql -u db_user -p'passw0rd' -D kashat -e "ALTER TABLE gallery_data      ADD PRIMARY KEY(db_matching_variable(240), Epoch(50));"
#----------------------------------------------------------------------------------------------------------------------