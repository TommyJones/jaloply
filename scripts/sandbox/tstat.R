
rm(list = ls())

source("scripts/00_common_functions.R")

load("data_derived/tsbs_raw.RData")

d <- tsbs[ sample(1:nrow(tsbs), 5000) , c("buldte", "maketxt", "compname") ]

str(d)

d$buldte <- as.Date(d$buldte, format = "%Y%m%d")

d$month <- lubridate::ceiling_date(as.Date(format(d$buldte, "%Y-%m-02")), "month") - 1

d <- d[ format(d$month, "%Y") %in% 1970:as.numeric(format(Sys.Date(), "%Y")) &
          ! is.na(d$month) , ]

date_fill <- seq(min(d$month), max(d$month), by = "day")

date_fill <- unique(lubridate::ceiling_date(as.Date(format(date_fill, "%Y-%m-02")), "month") - 1)

d <- merge(d, data.frame(month = date_fill, stringsAsFactors = F),
           all = TRUE)

d$component <- stringr::str_replace_all(d$compname, "\\d", "")
d$component <- sapply(strsplit(d$component, split = ":"), function(x) x[ 2 ])

d$count <- 1

d2 <- reshape2::dcast(d, month + component + maketxt ~ count, 
                     value.var = "count",
                     fun.aggregate = sum)

names(d2)[ names(d2) == "1" ] <- "count"

count1 <- by(d2, INDICES = d2$maketxt, function(x){
  
})

CalcSums <- function(d, time_var, platform_var, component_var) {
  
  
  
}

