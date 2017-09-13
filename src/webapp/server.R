# Load the ggplot2 package which provides
# the 'mpg' dataset.
library(ggplot2)

results_table <- read.csv('WebApp_placeholder_data')

function(input, output) {

  # Filter data based on selections
  output$table <- DT::renderDataTable({
    DT::datatable(
      results_table[, input$show_vars, drop = FALSE], 
      options = list(orderClasses = TRUE, lengthChange = FALSE),
      rownames= FALSE,
      filter = "top")
  })
}
