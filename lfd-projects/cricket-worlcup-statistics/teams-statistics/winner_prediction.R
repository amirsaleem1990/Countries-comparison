library(ranger)
library(rpart)
library(dplyr)
library(lubridate)
library(plyr)
library(caTools)

rm(list = ls())
setwd("/home/amir/LFD-server/cricket-worlcup-statistics/teams-statistics")
df = data.table::data.table(read.csv("ODI_matches_data.csv"))
df$Result <- ifelse(df$Result == "won" , 1, 0)
df$Year<- lubridate::year(lubridate::dmy(df$Start.Date))
df <- df %>% select(-c(X, X.1, Start.Date, Toss, Bat, Margin, BR))
df$Ground <- tolower(df$Ground)
df_min_year <- min(df$Year)
df$Year <- df$Year - df_min_year + 1
mapping <- list(AFR = c("WI", "ZIM", "Nairobi (Gym)"),
                ASIA = c("INDIA", "SL", "BDESH", "UAE", "Dubai (DSC)", "SA", "Hyderabad (Deccan)", "Colombo (SSC)",
                         "ICCA Dubai", "PAK", "Mumbai (BS)", "MAL"),
                EUROPE = c("SCOT", "NL", "Dublin (Malahide)", "The Oval", "IRE", "ENG", "King City (NW)", "CAN"),
                AUSTRALIA = c("NZ", "AUS")
                )

df$continent <- gsub('[[:digit:]]+', '', mapvalues(df$Ground_country, 
                                                      from=unlist(mapping),
                                                      to=names(unlist(mapping))
                                                      ))
df <- df %>% 
  select(-Ground)

WC_grounds <- data.table::data.table(read.csv("../K/WC_2019_matches_winners_grounds_and_teams.csv"))
WC_grounds$Ground <- tolower(lapply(
  strsplit(as.character(WC_grounds$Stadium),","), 
  function(x) x[2]))
WC_grounds$Ground_country <- "ENG"
WC_grounds$continent <- "EUROPE"
WC_grounds$Ground <- trimws(WC_grounds$Ground)
WC_grounds <- data.table::data.table(WC_grounds %>%
  select(-c("Stadium", "Date")))
WC_grounds$Year <- 2019 - df_min_year + 1
names(WC_grounds) <- c("Team", "Opposition", "Result", 'Ground', 'Ground_country',  'continent',  'Year')
WC_grounds <- WC_grounds %>% 
  filter(!Result == "Match abandoned without a ball bowled")
WC_grounds$Result <- trimws(WC_grounds$Result)
WC_grounds$Result <- as.numeric(as.character(WC_grounds$Result) == as.character(WC_grounds$Team))
WC_Result <- WC_grounds$Result
WC_grounds <- WC_grounds %>% 
  select(c(Team, Result, Opposition, Ground_country, Year, continent))


# split<-sample.split(df$Result,SplitRatio = 0.8)
# train<-subset(df, split==TRUE)
# test<-subset(df, split==FALSE)

train <- df
test <- WC_grounds

# model_ranger <- ranger(Result ~ ., data = train, num.trees = 1500, importance = "impurity",probability = T)
# pred_ranger <- predict(object = model_ranger, data = test, type = "response")$predictions[,1]
# sum(test$Result == ifelse(pred_ranger < 0.48, 0, 1)) / nrow(test)


model_rpart <- rpart(Result ~ ., data = train, control = rpart.control(cp= 0.001))
length(unique(model_rpart$where))
pred_rpart <- predict(model_rpart, newdata = test)
sum(test$Result == ifelse(pred_rpart < 0.33, 0, 1)) / nrow(test)
maxx <- 0
ii <- 0
for (i in seq(0.1, 1, 0.005)){
  if (sum(test$Result == ifelse(pred_rpart < i, 0, 1)) / nrow(test) > maxx){
    maxx <- sum(test$Result == ifelse(pred_rpart < i, 0, 1)) / nrow(test)
    ii <- i
  }
}
maxx
