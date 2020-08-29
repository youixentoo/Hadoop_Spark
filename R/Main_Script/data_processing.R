# Author: Thijs Weenink

# Main script for data processing, most functions can be found as individual running functions in the other .R files.
# Where they can be found is placed behind the function call or origin in this file.

library(sparklyr)
library(dplyr)

options(run.main=FALSE) # Prevents source() from running the function in the mentioned script.
source("R/Learning/connection.R")
options(run.main=TRUE)


# Main function that calls all other functions.
main = function(){
  vars = tryCatch({
    spark_connection(FALSE, FALSE) # From: connection.R
  }, error=function(cond){
    message("Disregard the message above.")
    spark_connection(TRUE, FALSE)
  }) # As you need overwrite the data to set-up connection if the data on Spark already exists. (Placeholder, currently don't know a better way)

  tail_freq = tailnum_frequency(vars$dataset)
  tail_dep_delay = tailnum_avg_delay(vars$dataset, vars$sc, "dep")
  tail_arr_delay = tailnum_avg_delay(vars$dataset, vars$sc, "arr")

  data = 
    inner_join(tail_freq, tail_dep_delay) %>%
    avg_delay_flight("dep") %>%
    inner_join(tail_arr_delay) %>%
    avg_delay_flight("arr")
  
  spark_data = copy_to(vars$sc, data, "processed_tails", overwrite = TRUE) # Copy data to Spark
  
  t = collect(spark_data)
  
  print(t)
}


# Determines the frequency of each tailnumber in the dataset
tailnum_frequency = function(dataset){ # From: count_tails_internal.R
  count_tailnum_unique =
    dataset %>%
    spark_dataframe() %>%
    invoke("orderBy", "tailnum", list()) %>%
    invoke("groupBy", "tailnum", list()) %>%
    invoke("count") %>%
    sdf_register() %>%
    return()
}


# Calculates the average delay for each unique tailnumber.
# Type determines if departure or arrival gets chosen,
# 'arr' for Arrival, anything else for Departure.
tailnum_avg_delay = function(dataset, sc, type){ # From: other_spark.R
  if(type=="arr"){
    tailnum_delay_avg = 
      dataset %>%
      spark_dataframe() %>%
      invoke("orderBy", "tailnum", list()) %>%
      invoke("groupBy", "tailnum", list()) %>%
      invoke("agg", invoke_static(sc, "org.apache.spark.sql.functions", "expr", "avg(arr_delay)"), list()) %>%
      sdf_register() %>%
      rename(AvgArrDelay = `avg(arr_delay)`) %>%
      return()
  }else{
    tailnum_delay_avg = 
      dataset %>%
      spark_dataframe() %>%
      invoke("orderBy", "tailnum", list()) %>%
      invoke("groupBy", "tailnum", list()) %>%
      invoke("agg", invoke_static(sc, "org.apache.spark.sql.functions", "expr", "avg(dep_delay)"), list()) %>%
      sdf_register() %>%
      rename(AvgDepDelay = `avg(dep_delay)`) %>%
      return()
  }
}


# Calculates the average for the type of delay per flight per tailnumber, adds it as a new row to the data
# Type determines if departure or arrival gets chosen,
# 'arr' for Arrival, anything else for Departure. 
avg_delay_flight = function(processed_data, type){
  if(type=="arr"){
    processed_data = processed_data %>% mutate(AAD_flight = AvgArrDelay/count)
  }else{
    processed_data = processed_data %>% mutate(ADD_flight = AvgDepDelay/count)
  }
  return(processed_data)
}


# Best equivalent to 'if __name__ == "__main__":' in Python I could find.
if (getOption('run.main', default=TRUE)) {
  main()
}
