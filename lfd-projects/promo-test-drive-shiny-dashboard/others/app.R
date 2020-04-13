setwd("~/.github/LFD-server/shiny/")
file_name = "lfd-and-unilever-logo.png"
rm(list = ls())
library(xlsx)
library(shiny)
library(ggplot2)
library(dplyr)
library(reshape2)
library(shinythemes)
data1 <- read.xlsx("Unilever Promotional Tools Demo - Jul 12 2019.xlsx", sheetIndex=1, header=TRUE)
data2 <- read.xlsx("Unilever Promotional Tools Demo - Jul 12 2019.xlsx", sheetIndex=2, header=TRUE)
data3 <- read.xlsx("Unilever Promotional Tools Demo - Jul 12 2019.xlsx", sheetIndex=3, header=TRUE)
data4 <- read.xlsx("Unilever Promotional Tools Demo - Jul 12 2019.xlsx", sheetIndex=4, header=TRUE)
ui = shinyUI(fluidPage(
fluidRow(tags$img(src= z)),
  fluidRow(
    column(width = 3,
           sliderInput(inputId = "slider1", 
                       h3("Marketting Duration[Months]"),
                       min = 1, max = 12, value = 1)),
      sidebarPanel(
                   column(5,
                          selectizeInput(inputId = "cnt",
                                  label = "Select Brand:",
                                  choices = levels(data1$Brand.List),
                                  selected = NULL))),
      sidebarPanel(
                   column(8,
                          selectizeInput(inputId = "cnt2",
                                  label = "Select Marketing tool",
                                  choices = levels(data1$Marketting.Tool),
                                  selected = NULL
                                  # multiple = TRUE,
                                  # options = list(placeholder = 'select a tool')
                   ))),
    column(width=10,offset=0, plotOutput("plot")),
    column(width=5,offset=5, textOutput("text.Output")))))
server = function(input, output, session) {
  observeEvent(input$cnt,{
    updateSelectInput(session,"cnt2")})
  output$plot = shiny::renderPlot({
    df <- data4 %>% filter(Brand == input$cnt)
    multi_factor <- data2[data2$Marketing.Channel == input$cnt2, c(2,3)]
    df['Expected_sales'] = c(df$Monthly.Average.Sales * multi_factor$Multi.Factor)
    # print(data3[data3$Months == input$slider1 & data2$Marketing.Channel == data3$Marketing.Channel, "Endurance"])
    
    ggplot(melt(df), aes(Channel, value)) +
      geom_bar(stat = "identity", aes(fill = variable), position = "dodge") +
      ggtitle("SALES COMPARISON!") +  # for the main title
      xlab("CHANNELS") +   ylab("SALES") + 
      theme_grey(base_size = 24) + 
      theme(
        plot.title = element_text(color="red", size=30, face="bold.italic", hjust = 0.5),
        axis.text.x = element_text(face="bold", color="#993333"),
        axis.text.y = element_text(face="bold", color="#993333"),
        axis.title=element_text(size=22,face="bold", color = "black")
        )
  }, width = 1900, height = 500)
  output$text.Output = renderText({
    paste(data3[data3$Months == input$slider1 & data2$Marketing.Channel == data3$Marketing.Channel, "Endurance"])
  })}
  
# run app in rstuidio local viewer
runApp(list(ui = ui, server = server))#, launch.browser = rstudioapi::viewer)


# Run app in browser
# runApp(list(ui = ui, server = server),host="127.0.0.1",port=4267, launch.browser = TRUE)
# PROMO TEST DRIVE