# Author: Thijs Weenink

# Script for counting the unique tailnums with R methods, only using Spark as initial data storage.

# Counter of counts_tails_spark.R

library(sparklyr)
library(dplyr)


start_time_int = Sys.time()
unique_tails_internal = 
  select(v_flights, tailnum) %>% # Select the data from Spark
  collect() %>% # Retrieves it from Spark
  table() %>% # Counts occurences of each unique tailnumber
  tibble::as_tibble() # Converts it to a tbl_df
end_time_int = Sys.time()


print(unique_tails_internal)
print(end_time_int - start_time_int)