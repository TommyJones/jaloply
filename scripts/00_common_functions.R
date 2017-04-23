################################################################################
# This script defines some common functions for use in this project
################################################################################

### Stick libraries used here --------------------------------------------------
library(stringr)
library(lubridate)
library(reshape2)
library(httr)

### Functions go here. Put a description in the comments -----------------------

# recursive_rbind <- function(list_object){
#   # this function takes a list and performs rbind in a recursive way
#   # recursive makes it faster and more memory efficient
#   
#   if(length(list_object) <= 1000)
#     return(do.call(rbind, list_object))
#   
#   batches <- seq(1, length(list_object), by = 1000)
#   
#   list_object <- lapply(batches, function(x){
#     do.call(rbind, list_object[ x:(min(x + 999, length(list_object))) ])
#   })
#   
#   recursive_rbind(list_object)
# }