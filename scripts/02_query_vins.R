################################################################################
# This script executes batch vin queries to get standardized make/model names
# for cmpl (and possibly other data sets)
################################################################################

rm(list = ls())

source("scripts/00_common_functions.R")

### Declare a helper function --------------------------------------------------
GetVinBatch <- function(vins){
  # uses httr::POST to get a batch of vins. 
  # there is a maximum batch size (I don't know what it is though)
  # returns a single character vector that is parseable as a csv
  
  vins <- paste(vins, collapse = ";")
  
  url <- "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/"
  
  body <- list(format = "csv", 
               data = vins)
  
  result <- POST(url = url, body = body, 
                 encode = "form")
  
  result <- content(result, "text")
  
  result
}

### Get a unique list of vin numbers from NHTSA data ---------------------------

load("data_derived/cmpl_raw.RData")

# keep only 10-digit or greater vins
vin_list <- unique(toupper(cmpl$vin[ nchar(cmpl$vin) >= 10 ]))

vin_list <- vin_list[ ! is.na(vin_list) ]

vin_list <- substr(vin_list, start = 1, stop = 10)

vin_list <- sort(unique(vin_list))

rm(cmpl, cmpl_info)
gc()

# divide into batches 
step_size <- 5000

batches <- seq(1, length(vin_list), by = step_size)

vin_list <- lapply(batches, function(x) vin_list[ x:min(x + step_size - 1, length(vin_list)) ])

# let 'er rip! 
vin_info <- lapply(vin_list, function(x){
  Sys.sleep(0.5)
  GetVinBatch(vins = x)
})

# Get the results out in a nice data frame
vin_info <- parallel::mclapply(vin_info, function(x){
  read.csv(textConnection(x), colClasses = "character")
}, mc.cores = 4)

