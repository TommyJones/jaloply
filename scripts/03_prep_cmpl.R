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

vin_merge$vin_make <- gsub(",.+$", "", vin_merge$vin_make) # get rid of repeat/multiple makes

cmpl$vin <- substr(cmpl$vin, 1, 10)

cmpl_formatted <- merge(cmpl, vin_merge)

cmpl_formatted$vin_make[ is.na(cmpl_formatted$vin_make) ] <- 
  toupper(cmpl_formatted$maketxt[ is.na(cmpl_formatted$vin_make) ])

cmpl_formatted$vin_model[ is.na(cmpl_formatted$vin_model) ] <- 
  toupper(cmpl_formatted$modeltxt[ is.na(cmpl_formatted$vin_model) ])

cmpl_formatted$vin_modelyear[ is.na(cmpl_formatted$vin_modelyear) | 
                                cmpl_formatted$vin_modelyear == ""] <- 
  toupper(cmpl_formatted$yeartxt[ is.na(cmpl_formatted$vin_modelyear) | 
                                    cmpl_formatted$vin_modelyear == ""])

cmpl_formatted$vin_modelyear[ as.numeric(cmpl_formatted$vin_modelyear) >  
                                as.numeric(format(cmpl_formatted$datea, "%Y")) &
                                ! cmpl_formatted$yeartxt == "" ] <-
  cmpl_formatted$yeartxt[ as.numeric(cmpl_formatted$vin_modelyear) >  
                            as.numeric(format(cmpl_formatted$datea, "%Y")) &
                            ! cmpl_formatted$yeartxt == "" ]

### Prepare for applying to the t-stat function --------------------------------

# platform and component variables
cmpl_formatted$platform <- paste(cmpl_formatted$vin_make, 
                                 cmpl_formatted$vin_model,
                                 sep = "_")

cmpl_formatted$component <- sapply(strsplit(cmpl_formatted$compdesc, split = ":"),
                                   function(x) x[ 1 ])

cmpl_formatted$component <- gsub("FUEL SYSTEM, [A-Z]+$", "FUEL SYSTEM", 
                                 cmpl_formatted$component)

cmpl_formatted$component <- gsub("FUEL/PROPULSION", "FUEL", 
                                 cmpl_formatted$component)


cmpl_formatted$component <- gsub("SERVICE BRAKES, [A-Z]+$", "SERVICE BRAKES", 
                                 cmpl_formatted$component)

cmpl_formatted$component[ is.na(cmpl_formatted$component) ] <- "UNKNOWN OR OTHER"

### Apply the t-stat functions -------------------------------------------------

cmpl_tstat <- CalcRollingCount(data = cmpl_formatted,
                               date_var = "month",
                               platform_var = "platform",
                               component_var = "component",
                               window = 365,
                               cpus = 3)

cmpl_tstat$dstat <- CalcTStatistic(n1 = cmpl_tstat$count,
                                   N1 = cmpl_tstat$platform_tot + 1,
                                   n2 = cmpl_tstat$component_tot,
                                   N2 = cmpl_tstat$total + 1,
                                   cohend = TRUE)

cmpl_tstat$pdstat <- pnorm(cmpl_tstat$dstat)

### Save and and exit ----------------------------------------------------------

save(cmpl_formatted, cmpl_tstat, file = "data_derived/cmpl_tstat.RData")
