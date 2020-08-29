# Author: Thijs Weenink

# Script as a collection of functions to test individually. Part of how I learned to use Spark.

library(sparklyr)
library(dplyr)
library(DBI)


# ---- Counting of rows in dataset ----

# # Simple row count.
# row_count = 
#   select(v_flights, tailnum) %>% 
#   spark_dataframe() %>% 
#   sparklyr::invoke("count") # Only called as invoke() onwards.
# 
# print(row_count)

# ---- Summary of mean, stddev, min, max of dataset ----

# # Api call that "Computes statistics for numeric columns, including count, mean, stddev, min, and max."
# # as stated by the documentation. Not to be used within programs, only to explore the data.
# # Non-numeric columns removed as doing these calculations on them is pointless.
# num_col_only =
#   select_if(v_flights, is.numeric)
# 
# tbl_flights_summary =
#   num_col_only %>%
#   spark_dataframe() %>%
#   invoke("describe", as.list(colnames(num_col_only))) %>%
#   sdf_register()
# 
# print(tbl_flights_summary)


# ---- Average delay/tailnum ----

# # Calculates the average delay for each unique tailnum.
# # Can be changed to any other column by changing so in the first invoke() on line 34.
# # Or commented out for the overall average.
# tailnum_delay_avg = 
#   v_flights %>%
#   spark_dataframe() %>%
#   invoke("orderBy", "tailnum", list()) %>%
#   invoke("groupBy", "tailnum", list()) %>%
#   invoke("agg", invoke_static(sc, "org.apache.spark.sql.functions", "expr", "avg(dep_delay)"), list()) %>%
#   sdf_register()
#   #collect()
#   
# print(tailnum_delay_avg)
# 
# tailnum_count_avgD = inner_join(unique_count_api_spdf, tailnum_delay_avg) # Combining the counting of tailnums and the avg delay of them.


# ---- Set-like creation ----

# # Creation of a set-like [??x1] table of unique tailnums.
# unique_count = select(v_flights, tailnum) %>%
#   spark_dataframe() %>%
#   invoke("distinct") %>%
#   sdf_register()
# 
# print(unique_count)


# ---- Simple SQL statement ----

# # Simple example of extracting data from Spark using SQL. Stores the data as a data.frame.
# # Using the DBI library.
# 
# data_from_sql = dbGetQuery(sc, "SELECT * FROM flights LIMIT 10")