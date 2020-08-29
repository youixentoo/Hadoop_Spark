# Author: Thijs Weenink

# Counting the unique tailnums entirely with spark api calls. 
# Compares the speed between having a tbl_df and a tbl_spark as endpoint.

# Counter of count_tails_internal.R

library(sparklyr)
library(dplyr)


start_time_int = Sys.time()
unique_count_api = 
  v_flights %>%
  spark_dataframe() %>%
  invoke("orderBy", "tailnum", list()) %>% # Only here to get it in the same order as the non-api method. Should be disabled when measuring speed.
  invoke("groupBy", "tailnum", list()) %>%
  invoke("count") %>% 
  collect() # %>%
  # na.omit() # To remove the NA value if necessary
end_time_int = Sys.time()

print("tbl_df:")
print(end_time_int - start_time_int)
# print(unique_count_api)


# Makes a tbl_spark instead of a tbl_df. This is much faster than the one above.
start_time_int = Sys.time()
unique_count_api_spdf =
  v_flights %>%
  spark_dataframe() %>%
  invoke("orderBy", "tailnum", list()) %>% # Only here to get it in the same order as the non-api method. Should be disabled when measuring speed.
  invoke("groupBy", "tailnum", list()) %>%
  invoke("count") %>%
  sdf_register()
end_time_int = Sys.time()

print("tbl_spark:")
print(end_time_int - start_time_int)
# print(unique_count_api_spdf)



