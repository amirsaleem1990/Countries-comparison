library(data.table)
library(lubridate)
library(dplyr)
countries = c("India", "Bangladesh", "England", "Australia", "Pakistan", "South Africe",
              "Afghanistan", "New Zealand", "West Indies", "Sri Lanka")
bowler <- data.table(read.csv("data/Bowler_data.csv"))
bastman <- data.table(read.csv("data/Batsman_Data.csv"))

bowler <- bowler %>% select(-X)
bastman <- bastman %>% select(-X)

bowler$Start.Date <- lubridate::dmy(bowler$Start.Date)
bastman$Start.Date <- lubridate::dmy(bastman$Start.Date)



bowler$Opposition <- gsub("^v ", "", bowler$Opposition)
bastman$Opposition <- gsub("^v ", "", bastman$Opposition)

# bowler <- bowler %>% filter(Opposition %in% countries)
# bastman <- bastman %>% filter(Opposition %in% countries)

bowler <- data.table(bowler)
bastman <- data.table(bastman)


# select only last 20 matches for each player
bowl_last_20 <- bowler %>%
  arrange(desc(Start.Date)) %>%
  group_by(Player_ID) %>%
  slice(c(1:20))

bat_last_20 <- bastman %>%
  arrange(desc(Start.Date)) %>%
  group_by(Player_ID) %>%
  slice(c(1:20))


# Convert Factors to Integers
bowl_last_20[, names(bowl_last_20)[1:7]] <-
  lapply(bowl_last_20[names(bowl_last_20)[1:7]],
         function(x) as.numeric(gsub("-", "", x)))

bat_last_20[, names(bat_last_20)[1:6]] <-
  lapply(bat_last_20[, names(bat_last_20)[1:6]],
       function(x) as.numeric(x))

summarised_bow_last_20 <- bowl_last_20 %>% 
  group_by(Player_ID) %>% 
  summarise(Overs = sum(Overs, na.rm=T), 
            Mdns = sum(Mdns, na.rm=T), 
            Runs = sum(Runs, na.rm=T), 
            Wkts = sum(Wkts, na.rm=T), 
            Ave = sum(Ave, na.rm=T), 
            SR = sum(SR, na.rm=T))

summarised_bat_last_20 <- bat_last_20 %>% 
  group_by(Player_ID) %>% 
  summarise(Bat = sum(Bat1, na.rm = T),
            Runs = sum(Runs, na.rm = T),
            BF = sum(BF, na.rm = T),
            SR = sum(SR, na.rm=T),
            X4s = sum(X4s, na.rm=T),
            X6s = sum(X6s, na.rm=T))

c <- cor(summarised_bat_last_20)
factors_data <- fa(r = c, nfactors = 6)
