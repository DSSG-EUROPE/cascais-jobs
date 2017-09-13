# Load the ggplot2 package which provides
# the 'mpg' dataset.
library(ggplot2)

results_table <- read.csv('WebApp_placeholder_data')

fluidPage(
  titlePanel("LTU Risk Prediction - Webapp"),
  sidebarLayout(
    sidebarPanel(
      checkboxGroupInput('show_vars', 'Columns to show:',
                         names(results_table), selected = names(results_table)),
      width = 2
    ),
    mainPanel(
      tabsetPanel(
        id = 'dataset',
        tabPanel('Users List', DT::dataTableOutput('table'))
      )
    )
  )
)
