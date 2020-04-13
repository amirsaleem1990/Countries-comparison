library(dplyr)
library(data.table)
library(caTools)

df <- read.csv("data/ODI_Match_Results.csv")
df <- data.table::as.data.table(df[, 2:ncol(df)])
library(lubridate)
df$Start.Date <- lubridate::dmy(df$Start.Date)
df$Opposition <- gsub("^v ", "", df$Opposition)
df <- df %>% filter(Opposition %in% unique(df$Country))
df <- df %>% filter(Result %in% c("lost", "won"))
Margin <- df$Margin
df$year <- year(df$Start.Date)

df$year <- df$year - min(df$year)+1
df <- df %>% select(-c(BR, Match_ID, Country_ID, Margin, Toss, Bat, Start.Date))
df$Result <- ifelse(df$Result == "won", 1, 0)

split<-sample.split(df$Result,SplitRatio = 0.8)
train<-subset(df, split==TRUE)
test<-subset(df, split==FALSE)

 #LM
# model1 <- lm(Result ~ ., data=train)
# summary(model1)


# Rpart
# library(rpart)
# fit <- rpart(Result ~ . , method="class", data=df)
# plot(fit, uniform=TRUE,main="Classification Tree for Kyphosis")
# text(fit, use.n=TRUE, all=TRUE, cex=.8)
# pred <- predict(object = fit, newdata = test)[,1]


# ranger
library(ranger)
model_ranger <- ranger(Result ~ ., data = train)
pred_ranger <- predict(object = model_ranger, data = test, type = "response")$predictions
sum(test$Result == ifelse(pred_ranger < 0.39, 0, 1)) / nrow(test)
# 0.6896552