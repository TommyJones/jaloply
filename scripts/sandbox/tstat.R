

rm(list = ls())

source("scripts/00_common_functions.R")

load("data_derived/cmpl_raw.RData")

data <- cmpl[ , c("datea", "maketxt", "modeltxt", "compdesc") ]

data$datea <- as.Date(data$datea, format = "%Y%m%d")

data$month <- lubridate::ceiling_date(as.Date(format(data$datea, "%Y-%m-02")), "month") - 1

data$component <- sapply(strsplit(data$compdesc, split = ":"), function(x) x[ 1 ])

data$platform <- toupper(stringr::str_conv(paste(data$maketxt, data$modeltxt), "UTF-8"))

date_fill <- seq(min(data$month, na.rm = T), max(data$month, na.rm = T), by = "day")

date_fill <- unique(lubridate::ceiling_date(as.Date(format(date_fill, "%Y-%m-02")), "month") - 1)

data <- merge(data, data.frame(month = date_fill, stringsAsFactors = F),
           all = TRUE)

counts <- CalcRollingCount(data = data[ nchar(data$platform) < 50 , ], date_var = "month", platform_var = "platform",
                           component_var = "component")

counts$t_stat <- CalcTStatistic(n1 = counts$count, N1 = counts$platform_tot,
                                n2 = counts$component_tot, N2 = counts$total,
                                scale = 800)

counts$d_stat <- CalcTStatistic(n1 = counts$count, N1 = counts$platform_tot,
                                n2 = counts$component_tot, N2 = counts$total,
                                cohend = TRUE)
