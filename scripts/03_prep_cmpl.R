################################################################################
# This script cleans the complaints data and applies the count and t-statistic
# functions to get our indicator variables
################################################################################

rm(list = ls())

source("scripts/00_common_functions.R")

load("data_derived/cmpl_raw.RData")

load("data_derived/vin_info.RData")

### Clean up our date variables ------------------------------------------------
cmpl$datea <- as.Date(cmpl$datea, format = "%Y%m%d")

cmpl <- cmpl[ ! is.na(cmpl$datea) , ]

cmpl$month <- lubridate::ceiling_date(as.Date(format(cmpl$datea, "%Y-%m-02")), "month") - 1

### Get clean make/model/year from vin queries ---------------------------------

vin_merge <- vin_info[ , c("vin", "make", "model", "modelyear") ]

vin_merge[ vin_merge == "" ] <- NA

vin_merge <- vin_merge[ ! is.na(vin_merge$vin) & ! is.na(vin_merge$make) &
                          nchar(vin_merge$vin) == 10 , ]

vin_merge$make <- toupper(vin_merge$make)
vin_merge$model <- toupper(vin_merge$model)

names(vin_merge)[ 2:4 ] <- paste0("vin_", names(vin_merge[ 2:4 ]))

cmpl$vin <- substr(cmpl$vin, 1, 10)

cmpl_formatted <- merge(cmpl, vin_merge)

cmpl_formatted$vin_make[ is.na(cmpl_formatted$vin_make) ] <- 
  toupper(cmpl_formatted$maketxt[ is.na(cmpl_formatted$vin_make) ])

cmpl_formatted$vin_model[ is.na(cmpl_formatted$vin_model) ] <- 
  toupper(cmpl_formatted$modeltxt[ is.na(cmpl_formatted$vin_model) ])

cmpl_formatted$vin_modelyear[ is.na(cmpl_formatted$vin_modelyear) ] <- 
  toupper(cmpl_formatted$yeartxt[ is.na(cmpl_formatted$vin_modelyear) ])

### Prepare for applying to the t-stat function --------------------------------

# platform and component variables
cmpl_formatted$platform <- paste(cmpl_formatted$vin_make, 
                                 cmpl_formatted$vin_model,
                                 sep = "_")

cmpl_formatted$component <- sapply(strsplit(cmpl_formatted$compdesc, split = ":"),
                                   function(x) x[ 1 ])

cmpl_formatted$component <- gsub("FUEL SYSTEM, [A-Z]+$", "FUEL SYSTEM", 
                                 cmpl_formatted$component)

cmpl_formatted$component <- gsub("SERVICE BRAKES, [A-Z]+$", "SERVICE BRAKES", 
                                 cmpl_formatted$component)

cmpl_formatted$component[ is.na(cmpl_formatted$component) ] <- "UNKNOWN OR OTHER"

# for each platform, get the earliest date of record
# first_date <- cmpl_formatted[ , c("datea", "platform") ]
# first_date <- first_date[ ! duplicated(first_date) , ]
# first_date <- by(first_date, INDICES = first_date$platform, function(x){
#   data.frame(first_date = x$datea[ which.min(x$datea) ],
#              platform = x$platform[ 1 ],
#              stringsAsFactors = FALSE)
# })
# first_date <- recursive_rbind(first_date)

# fill in dates, platforms, and components
# date_fill <- seq(min(cmpl_formatted$month, na.rm = T), 
#                  max(cmpl_formatted$month, na.rm = T), by = "day")
# 
# date_fill <- unique(lubridate::ceiling_date(as.Date(format(date_fill, "%Y-%m-02")), "month") - 1)
# 
# date_fill <- data.frame(month = date_fill, stringsAsFactors = F)
# 
# platform_fill <- data.frame(platform = unique(cmpl_formatted$platform),
#                             stringsAsFactors = F)
# 
# component_fill <- data.frame(component = unique(cmpl_formatted$component),
#                              stringsAsFactors = FALSE) # using this in next section
# 
# fill_mat <- merge(date_fill, platform_fill, all = TRUE)
# 
# fill_mat <- merge(fill_mat, first_date)
# 
# fill_mat <- fill_mat[ fill_mat$month >= fill_mat$first_date , ] # remove entries before car "existed"
# 
# cmpl_formatted <- merge(cmpl_formatted, 
#                         fill_mat,
#                         all = TRUE)
# 
# rm(fill_mat)
# gc()

### Apply the t-stat functions -------------------------------------------------

cmpl_tstat <- CalcRollingCount(data = cmpl_formatted,
                               date_var = "month",
                               platform_var = "platform",
                               component_var = "component",
                               window = 365,
                               cpus = 3)

cmpl_tstat$dstat <- CalcTStatistic(n1 = cmpl_tstat$count,
                                   N1 = cmpl_tstat$platform_tot,
                                   n2 = cmpl_tstat$component_tot,
                                   N2 = cmpl_tstat$total,
                                   cohend = TRUE)

cmpl_tstat$pdstat <- pnorm(cmpl_tstat$dstat)

### Remove all entries prior to first complaint in the database ----------------
