################################################################################
# This file downloads and unpacks NHTSA files for later use downstream
################################################################################

rm(list = ls())

d <- gsub("-", "_", Sys.Date()) # get today's date for versioning

### Download the files ---------------------------------------------------------

# complaints data
download.file(url = "https://www-odi.nhtsa.dot.gov/downloads/folders/Complaints/FLAT_CMPL.zip", 
              destfile = paste0("data_raw/cmpl_", d, ".zip"),
              method = "auto", mode = "wb")

# complaints headers
download.file(url = "https://www-odi.nhtsa.dot.gov/downloads/folders/Complaints/CMPL.txt", 
              destfile = paste0("data_raw/cmpl_", d, ".txt"),
              method = "auto", mode = "wb")

# recall data
download.file(url = "https://www-odi.nhtsa.dot.gov/downloads/folders/Recalls/FLAT_RCL.zip", 
              destfile = paste0("data_raw/rcl_", d, ".zip"),
              method = "auto", mode = "wb")

# recall headers
download.file(url = "https://www-odi.nhtsa.dot.gov/downloads/folders/Recalls/RCL.txt", 
              destfile = paste0("data_raw/rcl_", d, ".txt"),
              method = "auto", mode = "wb")

# investigations data
download.file(url = "https://www-odi.nhtsa.dot.gov/downloads/folders/Investigations/FLAT_INV.zip", 
              destfile = paste0("data_raw/inv_", d, ".zip"),
              method = "auto", mode = "wb")

# investigations headers
download.file(url = "https://www-odi.nhtsa.dot.gov/downloads/folders/Investigations/INV.txt", 
              destfile = paste0("data_raw/inv_", d, ".txt"),
              method = "auto", mode = "wb")

# technical service bulletin data
download.file(url = "https://www-odi.nhtsa.dot.gov/downloads/folders/TSBS/FLAT_TSBS.zip", 
              destfile = paste0("data_raw/tsbs_", d, ".zip"),
              method = "auto", mode = "wb")

# technical service bulletin
download.file(url = "https://www-odi.nhtsa.dot.gov/downloads/folders/TSBS/TSBS.txt", 
              destfile = paste0("data_raw/tsbs_", d, ".txt"),
              method = "auto", mode = "wb")

### Unzip the files ------------------------------------------------------------
ziplist <- list.files(path = "data_raw", 
                      pattern = paste0(d, "\\.zip"),
                      full.names = TRUE)

lapply(ziplist, function(x){
  unzip(zipfile = x,
        exdir = "data_raw")
})

### Import into R and do very basic formatting ---------------------------------

# function to load the flat files in a clean way.
ImportFlatFile <- function(filepath){
  d <- scan(filepath, what = "character", sep = "\n")
  d <- stringr::str_conv(d, "UTF-8")
  
  write.table(d, "../tmp.txt", sep = "\t", quote = T, row.names = F, col.names = F)
  d <- read.delim(filepath, sep = "\t", colClasses = "character", header = F)
  file.remove("../tmp.txt")
  d
}

# complaints
cmpl <- ImportFlatFile("data_raw/FLAT_CMPL.txt")

cmplnames <- scan(paste0("data_raw/cmpl_", d, ".txt"), 
                  what = "character",
                  sep = "\n")

cmplnames <- grep(pattern = "^\\d{1} |^\\d{2} ", cmplnames, value = T)

cmplnames <- strsplit(cmplnames, split = " ")

names(cmpl) <- sapply(cmplnames, function(x){
  x <- x[ x != "" ]
  x[ 2 ]
})

cmplnames <- sapply(cmplnames, function(x) paste(x[ x != "" ], collapse = " "))

# recalls

# investigations

# TSBs
