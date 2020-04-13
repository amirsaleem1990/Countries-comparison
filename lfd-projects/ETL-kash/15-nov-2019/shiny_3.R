library(ggplot2)
library(shiny)
library(dplyr)
library(RColorBrewer)
library(DBI)

df = read.csv("logs.csv")
Corrupt_errors <- df[df$Error == "Corrupt folder error", ]
df <- df[df$Error != "Corrupt folder error", ]
df$Time <- as.POSIXct(as.character(df$Time), format="%Y-%m-%d %H:%M", tz = "UTC")
seconds_in_one_day <- 60 * 60 * 24
total_days <- round(as.numeric(max(df$Time) - min(df$Time)), 2) + 0.1

# # Files with Error(any) in %
con <- dbConnect(RSQLite::SQLite(), "ETL.db")
tables <- dbListTables(con)
ID <- c()
for (i in tables){
  adf <- dbReadTable(con, i)
  ID <- append(ID, paste(adf$ID, adf$Date...time))
}
unique_IDs_qty <- length(unique(ID))

plot.file.with.error.any <- function(){
	barplot(sort((table(df$File)/unique_IDs_qty * 100), decreasing = TRUE),
        main= "Files with Error(any) in %",
        ylab = "%",
        col=brewer.pal(5,"Set1"),border="white",
        las=2)
}
plot.func <- function(days, error_type){
	df_previous_N_days <- df[df$Time >= (max(df$Time) - (days * seconds_in_one_day)) , ]
	if (error_type != "All"){
	    df_previous_N_days <- df[df$Error == error_type, ]
	}
    IDs <- sapply(strsplit(as.character(df_previous_N_days$Folder), "-"), FUN = function(x){as.numeric(x[3])})

	ggplot(df_previous_N_days, aes(x = File, fill=Error)) +
	  geom_bar(position = "dodge") +
	  theme(text = element_text(size=30),
	        axis.text.x = element_text(angle = 90)) + 
	  # scale_y_discrete(limits = 0:length(unique(IDs))) + 
	  labs(title="Main Title",
	       x ="X Title", 
	       y = "Y Title") + 
      stat_count(aes(label = paste0(round(..count../length(unique(IDs)) *100), "%")), 
                 vjust = 1, geom = "text", position = "identity", color ="white", size=10)
    }

ui = shinyUI(fluidPage(  
  sidebarLayout(
    sidebarPanel(width = 2,
                 numericInput(inputId = "days.input", 
                 			  label = "Previous Days:", 
                              value = total_days, 
                              min = 0.1, 
                              max = total_days
                              ),
                 selectizeInput(inputId = "cnt",
                                label = "Select Error",
                                choices = c("All", levels(df$Error)[levels(df$Error) != "Corrupt folder error"]),
                                selected = "All")
                 ),
    mainPanel(
    	h3(textOutput("text.Output")),
    	plotOutput("plot"))
    )))

server = function(input, output, session) {
  # observeEvent(input$days.input,{
  #   updateSelectInput(session,"text.Output")})

	output$plot = shiny::renderPlot({
	# barplot.each.error.for.each.file.for.n.days <- function(df, Days){
	  # if(missing(days)) { # missing() to test whether or not the argument <days> was supplied
	  #   days <-  as.numeric(max(df$Time) - min(df$Time))
	  # }

	  plot.func(input$days.input, input$cnt)
	  }, width = 1700, height = 1000)

	output$text.Output = renderText({
		df_previous_N_days <- df[df$Time >= (max(df$Time) - (input$days.input * seconds_in_one_day)) , ]
		paste("Unique IDs: ", length(unique(sapply(strsplit(as.character(df_previous_N_days$Folder), "-"), 
			FUN = function(x){as.numeric(x[3])}))))
		})
}
runApp(list(ui = ui, server = server),host="127.0.0.1",port=3748, launch.browser = TRUE)
# shinyApp(ui = ui, server = server)
