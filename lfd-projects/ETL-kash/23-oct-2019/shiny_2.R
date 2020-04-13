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
errors = read.csv("logs.csv")

barplot(sort((table(errors$File)/unique_IDs_qty * 100), decreasing = TRUE),
        main= "Files with Error(any) in %",
        ylab = "%",
        col=brewer.pal(5,"Set1"),border="white",
        las=2)


ggplot(errors, aes(x = File, fill=Error)) + 
  geom_bar(position = "dodge") + 
  labs(title = "Files with Error")
