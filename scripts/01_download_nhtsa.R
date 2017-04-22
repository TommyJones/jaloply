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

FormatNames <- function(filepath){
  d <- scan(filepath, what = "character", sep = "\n")
  d <- grep(pattern = "^\\d{1} |^\\d{2} ", d, value = T)
  d <- strsplit(d, split = " ")
  d <- lapply(d, function(x) x[ x != "" ])
  
  names <- tolower(sapply(d, function(x) x[ 2 ]))
  
  dictionary <- lapply(d, function(x) c(x[ 1:3 ], paste(x[ 4:length(x) ], collapse = " ")))
  
  dictionary <- do.call(rbind, dictionary)
  
  colnames(dictionary) <- c("id", "name", "source_data_type", "description")
  
  dictionary <- as.data.frame(dictionary, stringsAsFactors = FALSE)
  
  list(names = names,
       dictionary = dictionary)
  
}

# complaints
cmpl <- ImportFlatFile("data_raw/FLAT_CMPL.txt")

cmpl_info <- FormatNames(paste0("data_raw/cmpl_", d, ".txt"))

names(cmpl) <- cmpl_info[[ 1 ]]

save(cmpl, cmpl_info, file = "data_derived/cmpl_raw.RData")

rm(cmpl, cmpl_info)
gc()

# recalls
rcl <- ImportFlatFile("data_raw/FLAT_RCL.txt")

rcl_info <- FormatNames(paste0("data_raw/rcl_", d, ".txt"))

names(rcl) <- rcl_info[[ 1 ]]

save(rcl, rcl_info, file = "data_derived/rcl_raw.RData")

rm(rcl, rcl_info)
gc()

# investigations
inv <- ImportFlatFile("data_raw/flat_inv.txt")

inv_info <- FormatNames(paste0("data_raw/inv_", d, ".txt"))

names(inv) <- inv_info[[ 1 ]]

save(inv, inv_info, file = "data_derived/inv_raw.RData")

rm(inv, inv_info)
gc()

# TSBs
tsbs <- ImportFlatFile("data_raw/flat_tsbs.txt")

tsbs_info <- FormatNames(paste0("data_raw/tsbs_", d, ".txt"))

names(tsbs) <- tsbs_info[[ 1 ]]

save(tsbs, tsbs_info, file = "data_derived/tsbs_raw.RData")

rm(tsbs, tsbs_info)
gc()
