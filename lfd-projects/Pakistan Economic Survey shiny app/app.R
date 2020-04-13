library(ggrepel)
library(shiny)
library(plotly)
library(reshape2)
library(shinythemes)
library(dplyr)
library(RColorBrewer)
l <- list(
  x = 0.05, 
  y = 0.98,
  # orientation = 'h',
  font = list(
    family = "sans-serif",
    size = 10,
    color = "#000"),
  bgcolor = "transparent",#E2E2E2",
  bordercolor = "#FFFFFF"
)
load('data.RData')
names(df)=c('samples','2000.01','2001.02','2002.03','2003.04','2004.05','2005.06','2006.07','2007.08','2008.09','2009.10','2010.11','2011.12','2012.13','2013.14','2014.15','2015.16','2016.17','2017.18','table')
df$samples <- c('Vocational | Female | (000)', 'Vocational | Total | (000)', 'Intermediate | Female | (000)',
                'Intermediate | Total | (000)', 'Grade IX-X | Female | (000)', 'Grade IX-X  | Total | (000)',
                'Grade VI-VIII | Female | (000)', 'Grade VI-VIII | Total | (000)', 'Grade I-V | Female | (000)', 
                'Grade I-V  | Total | (000)', 'University | Females', 'University | Total', 'Degree College | Females',
                'Degree College | Total', 'Female High Schools (000)', 'Female Middle Schools (000)', 'All High Schools (000)',
                'All Middle Schools (000)', 'Female Primary Schools (000)', 'All Universities', 'All Primary Schools (000)', 
                'Female Degree Colleges', 'Female Vocational Institutions', 'All Degree Colleges', 'All Vocational Institutions', 
                'Female Inter Colleges', 'All Inter colleges', 'Female High Schools (000)', 'Female Middle Schools (000)', 
                'Female Primary Schools (000)', 'All Middle Schools (000)', 'All High Schools (000)', 'All Primary Schools (000)', 
                'Female Vocational Institutions', 'Female Degree Colleges', 'All Universities', 'All Vocational Institutions',
                'All Degree Colleges', 'Female Inter Colleges', 'All Inter colleges')
df$table<-factor(df$table)
df$samples<-factor(df$samples)
ui = shinyUI(fluidPage(
  tags$head(tags$style(HTML("
        .selectize-input, .selectize-dropdown {
          font-size: 85%;
        }
        "))),
  tags$head(tags$script('
                        var dimension = [0, 0];
                        $(document).on("shiny:connected", function(e) {
                        dimension[0] = window.innerWidth;
                        dimension[1] = window.innerHeight;
                        Shiny.onInputChange("dimension", dimension);
                        });
                        $(window).resize(function(e) {
                        dimension[0] = window.innerWidth;
                        dimension[1] = window.innerHeight;
                        Shiny.onInputChange("dimension", dimension);
                        });
                        ')),
  
  
  
  theme = shinythemes::shinytheme("readable"),
                       title = 'Pakistan Economic Survey',
                  tags$div(class="outer",
                           tags$div(tags$img(src = 'https://scontent.fkhi10-1.fna.fbcdn.net/v/t1.0-1/p200x200/37768545_611102615949930_6085704092741533696_n.png?_nc_cat=0&oh=731effd2c4ecbca098b57f0be6e2652d&oe=5C0FA019',
                                height = 140,
                                width = 140),
                                style = "display:inline-block"),
                           tags$div(h1("PAKISTAN ECONOMIC SURVEY", 
                          style = "font-family: 'candara', cursive;
                          font-weight: 150; line-height: 1; text-align:center;
                          color: #4d3a7d; display:inline-block"),
                          style = "width:80%; display:inline-block; text-align: center;")
                        ),
                       sidebarLayout(
                         sidebarPanel(width = 2,
                                      selectizeInput("cnt", 
                                                     "Select Table:",
                                                     choices = levels(df$table),
                                                     selected = NULL),
                                      selectizeInput("cnt2",
                                                     "Select Metric:",
                                                     choices = c(),
                                                     selected = NaN,
                                                     multiple = TRUE)),
                         mainPanel(plotlyOutput("plot")))))
server = function(input, output, session) {
  df <- melt(df, id = c("samples", "table"))
  subdf<<-data.frame()
  observeEvent(input$cnt,{
    test<-df[df$table==input$cnt,]
    subdf<<-test
    selections<-unique(test$samples)
    updateSelectInput(session,"cnt2",choices = selections, selected = selections[[1]])})
  output$plot = renderPlotly({
    plot.data <- subdf[subdf$samples %in% input$cnt2, ]
    req(plot.data[1,1])
    plot_ly(data = plot.data, x = plot.data$variable, y = plot.data$value, width = 4,color = plot.data$samples, colors = c("red", "blue", "green", 'orange', 'purple'),type = 'scatter', mode = 'lines', marker = list(size = 10))%>% 
      layout(
        xaxis = list(
        ticklen = 7,
        showgrid = TRUE,
        gridwidth = 5
      ),
      legend = l,
      width = (0.8*as.numeric(input$dimension[1])),
      height = 0.75 * as.numeric(input$dimension[2])
      )
  })}
shinyApp(ui = ui, server = server)