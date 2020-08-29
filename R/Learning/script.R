# Example script for data extraction and usage

library(sparklyr)
library(dplyr)


spark_tail = select(flights, tailnum) # Select data from Spark

tails = collect(spark_tail) # Copy to R

count_tails = gather(tails,key="variable",na.rm=TRUE) %>% group_by(value) %>% summarise(n= n()) # Data manipulation

# Other things you might want to do
print(count_tails)

spark_cp_tails = copy_to(sc, count_tails, "counted") # Copy back to Spark

# Done