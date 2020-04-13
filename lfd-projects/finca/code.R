library(readxl)
library(dplyr)
library(stringi)
# library(Hmisc)
# since there is more than one sheet in our file and our intrest not in first sheet; we have to cpecify sheet.

# Read data from excel file
# sheet_name <- "Sheet1"
# data <- read_excel("data.xlsx", sheet = sheet_name)
# data <- data.frame(data)

# Read data from Rdata file (converted from excel file)

data <- readRDS(file = "data.rds")

for (i in names(data)){
  if (is.character(data[[i]])){
    data[, i] <- as.factor(tolower(data[[i]]))
}}


# adding Primary_key column, by murgin 2 existing columns
data['Primary_key'] <- paste(as.character(data$applicationnumber), as.character(data$loantype), sep="")
#
# count of <DataType>
count(data, DataType)
#
#
# srif 1 esi row h jis me <DataType> and <Primary_key> dono same hen.
data %>% count(DataType, Primary_key) %>% filter(n > 1)
#
# # -----------------------------------------------
# # Q1- There is a column for data type which includes only two values i.e. new and old. Please check if there are any loans in new data which also exist in old data. The key for the data is application ID and loan ID i.e. application ID + loan ID is your primary key.
#
# # Only duplicates
old_primary_keys <- data[data$DataType == "old", ]$Primary_key
new_primary_keys <- data[data$DataType == "new", ]$Primary_key
#
in_both_new_and_old <- data[data$Primary_key %in% old_primary_keys & data$Primary_key %in% new_primary_keys,]

print(in_both_new_and_old %>% group_by(DataType) %>% count())

cat("There is ", nrow(in_both_new_and_old)/2, " Primary keys, which is in both New and Old groups")
# # -----------------------------------------------
# # Q.2-  For any repeated loans, please check if the data in other columns is also the same
duplicated_old <- in_both_new_and_old[in_both_new_and_old$DataType == "old", ]
duplicated_old <- duplicated_old[order(duplicated_old$Primary_key),]
#
duplicated_new <- in_both_new_and_old[in_both_new_and_old$DataType == "new", ]
duplicated_new <- duplicated_new[order(duplicated_new$Primary_key),]
# drop <- c("DataType")
# data <- data[, !(names(data) %in% drop)]
#
merged <- merge(duplicated_old, duplicated_new, by = names(data))
cat("We have ", nrow(merged), " rows in our data than have only changings in DataType coulumn, all else same")
#
#
# #-----------------------------
#in_both_new_and_old <- in_both_new_and_old[order(in_both_new_and_old$Primary_key),]
# cbind(duplicated_old[order(duplicated_old$Primary_key),]$Primary_key,
matching <-  data.frame(duplicated_new == duplicated_old)
matching$Primary_key = duplicated_new$Primary_key

# names(matching) <- names(duplicated_new)
# View(matching)
write.csv(in_both_new_and_old, 'in_both_new_and_old.csv')
#
# ratio_of_same_entry_both_time
l = c()
for (i in names(matching)) {
  l <- append(l, c(i, sum(matching[[i]], na.rm = T), mean(matching[[i]], na.rm = T)))
}
ratio_of_same_entry_both_time = data.frame(matrix(l, byrow = T, ncol = 3))
ratio_of_same_entry_both_time[order(ratio_of_same_entry_both_time$X3), ]

# --------------------------------------------------------------------------------
# Q.3: I want you to compare variable distributions in new vs old data and document any significant changes

for (i in names(duplicated_new)){
  if (!is.character(duplicated_new[[i]])){
    print(i)
    # summary(duplicated_old[[i]]) == summary(duplicated_new[[i]])
    # par(mfrow=c(1,2))
    # hist(duplicated_new[[i]], main="New", breaks=seq(0, 3500, 100))
    # hist(duplicated_old[[i]], main="Old", breaks=seq(0, 3500, 100))
}}
library(purrr)
library(tidyr)
library(ggplot2)

duplicated_old %>%
  keep(is.numeric) %>% 
  gather() %>% 
  ggplot(aes(value)) +
  facet_wrap(~ key, scales = "free") +
  geom_histogram(binwidth = 20)

par(mfrow=c(1,2))
boxplot(duplicated_new$EMI, main="New")
boxplot(duplicated_old$EMI, main="Old")

# # NOTE: Q4 me loop old or new par loop chala kar har variable dono datasets me sy graph bana do