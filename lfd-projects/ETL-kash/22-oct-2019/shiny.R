library(DBI)

# connect to database
con <- dbConnect(RSQLite::SQLite(), "ETL.db")

tables <- dbListTables(con)

# variables names
dbListFields(con, "call_log")

# dataframe
dbReadTable(con, "call_log")


accounts_list  <- dbReadTable(con, "accounts_list")
app_install_log  <- dbReadTable(con, "app_install_log")
calendar_events <- dbReadTable(con, "calendar_events")
call_log <- dbReadTable(con, "call_log")
contacts_list  <- dbReadTable(con, "contacts_list")
ext_storage_files <- dbReadTable(con, "ext_storage_files")
filter_app_log <- dbReadTable(con, "filter_app_log")
location <- dbReadTable(con, "location")
outgoing_call_log <- dbReadTable(con, "outgoing_call_log")
sms_log  <- dbReadTable(con, "sms_log")
sms_sent_log <- dbReadTable(con, "sms_sent_log")

# library(hash)
# h <- hash() 
# for (i in tables){
#   h[[i]] <- dbReadTable(con, i)
# }

# barplot(table(accounts_list$name), 
#         main= "Accounts_list",
#         xlab= "Name",
#         las=2)

