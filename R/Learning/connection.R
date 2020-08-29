# Author: Thijs Weenink

# Connection to spark and copying data


# Needed for spark connection
library(sparklyr)
library(dplyr)

# Data
library(nycflights13)


# Function for setting up the connection to Spark.
# replace_old needs to be FALSE on the first run, as then it initialises Spark and copies the data from R to it.
# All other times replace_old need to be TRUE
# Internal specifies if the function is called from here (TRUE) or another script (FALSE)
# Determines if the variables need to be set globally or not. 
spark_connection = function(replace_old, internal=TRUE){
  if(!replace_old){
    message("Initialising Spark")
  }
  sc = spark_connect(master = "local")
  v_flights = copy_to(sc, flights, "flights", overwrite = replace_old) # Copy data to Spark
  
  # For when the script is run for standalone tests, instead of being called from another script.
  # Sets the 2 variables above as global.
  if(internal){ 
    assign("sc", sc, envir=.GlobalEnv)
    assign("v_flights", v_flights, envir=.GlobalEnv)
  }
  
  vars = list("sc"=sc, "dataset"=v_flights)
  return(vars)
}


# Best equivalent to 'if __name__ == "__main__":' in Python I could find.
# Prevents the main script from executing this when using source()
# Remember to change "FALSE" to "TRUE" if running this script and there is already a Spark connection.
if (getOption('run.main', default=TRUE)) {
  spark_connection(FALSE)
}
