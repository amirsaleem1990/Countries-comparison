library(ggrepel)
library(shiny)
library(ggplot2)
library(reshape2)
library(shinythemes)
df = read.csv('data.csv')
names(df)=c('samples','2000.01','2001.02','2002.03','2003.04','2004.05','2005.06','2006.07','2007.08','2008.09','2009.10','2010.11','2011.12','2012.13','2013.14','2014.15','2015.16','2016.17','2017.18','table')
df$table<-factor(df$table)
df$samples<-factor(df$samples)
ui = shinyUI(fluidPage(
  
  title = 'Pakistan Economic Servey',
  tags$br(),
  tags$img(src = 'http://www.lovefordata.com/wp-content/uploads/2016/05/Printable_Vector_File_2.png',
           height = 60,
           width = 125),
  # shinythemes::themeSelector(),
  
  tags$head(
    tags$style(HTML("
                    @import url('//fonts.googleapis.com/css?family=Lobster|Cabin:400,700');"))),
  headerPanel(
    h1("Pakistan Economic Survey", 
       style = "font-family: 'Lobster', cursive;
       font-weight: 150; line-height: 1; 
       color: #4d3a7d;text-align: center")),
  sidebarLayout(
    sidebarPanel(width = 2,
                 selectizeInput("cnt", 
                                "Select Table:",
                                choices = levels(df$table),
                                selected = NULL),
                 selectizeInput("cnt2",
                                "Select samples:",
                                choices = c(),
                                selected = NaN,
                                multiple = TRUE)),
    mainPanel(plotOutput("plot")))))
server = function(input, output, session) {
  df <- melt(df, id = c("samples", "table"))
  subdf<<-data.frame()
  observeEvent(input$cnt,{
    test<-df[df$table==input$cnt,]
    subdf<<-test
    selections<-unique(test$samples)
    updateSelectInput(session,"cnt2",choices = selections)})
  output$plot = renderPlot({
    plot.data <- subdf[subdf$samples %in% input$cnt2, ]
    title = 'Plot title'
    ggplot(data=plot.data,aes(x = variable, y = value, colour = samples, group = samples), size = 1.5) +
      geom_point() +
      geom_line() +
      labs (x = "Years", y = '', title = input$cnt) +
      theme(
        plot.title=element_text(hjust = 0.5,size=20, face="bold", family="American Typewriter",color="tomato",vjust = 0,lineheight=1.2),
        axis.line= element_line(colour = "black", size = 0.6),
        axis.text= element_text(size = 20, colour = "black"),
        legend.text= element_text(size = 16),
        legend.title= element_text(size = 0),
        axis.title.x = element_text(colour="#990000", size=20, vjust = -1.0),
        axis.text.x  = element_text(angle=90, vjust=0.5, size=16),
        axis.text.y  = element_text(hjust=0.5, size=16),
        panel.grid.major = element_line(colour = "gray"),
        axis.ticks.length = unit(0.75, "cm"),
        # legend.position = c(0.1, 0.8)
        legend.position="right"
        # legend.box = "vertical"
        # legend.position = "none"
    
      )+
    guides(colour = guide_legend(override.aes = list(size=6.0)))

  })}
shinyApp(ui = ui, server = server)

