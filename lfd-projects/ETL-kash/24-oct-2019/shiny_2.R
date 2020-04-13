library(ggplot2)
library(dplyr)
library(RColorBrewer)
library(DBI)
con <- dbConnect(RSQLite::SQLite(), "ETL.db")
tables <- dbListTables(con)
ID <- c()
for (i in tables){
  df <- dbReadTable(con, i)
  ID <- append(ID, paste(df$ID, df$Date...time))
}
unique_IDs_qty <- length(unique(ID))
df = read.csv("logs.csv")

# # Files with Error(any) in %
# barplot(sort((table(df$File)/unique_IDs_qty * 100), decreasing = TRUE),
#         main= "Files with Error(any) in %",
#         ylab = "%",
#         col=brewer.pal(5,"Set1"),border="white",
#         las=2)


barplot.each.error.for.each.file.for.n.days <- function(df, days){
  if(missing(days)) { # missing() to test whether or not the argument <days> was supplied
    days <-  as.numeric(max(df$Time) - min(df$Time))
  }
  df$Time <- as.POSIXct(as.character(df$Time), format="%Y-%m-%d %H:%M", tz = "UTC")
  seconds_in_one_day <- 60 * 60 * 24
  
  df_previous_N_days <<- df[df$Time > max(df$Time) - (days * seconds_in_one_day) , ]
  IDs <- sapply(strsplit(as.character(df_previous_N_days$Folder), "-"), FUN = function(x){as.numeric(x[3])})
  
  ggplot(df_previous_N_days, aes(x = File, fill=Error)) +
    geom_bar(position = "dodge") +
    scale_y_discrete(limits = 1:length(unique(IDs))) + 
    labs(title="Main Title",
         x ="X Title", 
         y = "Y Title") + 
    stat_count(aes(label = paste0(round(..count../length(unique(IDs)) *100), "%")),
               vjust = 1, geom = "text", position = "identity", color ="white")
}


barplot.each.error.for.each.file.for.n.days(df, 3)


