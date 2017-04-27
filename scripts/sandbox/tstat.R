
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


CalcRollingCount <- function(data, date_var, platform_var, component_var,
                             window = 365, cpus = 4){
  
  # Subset the data on only the columns we need
  data <- data[ , c(date_var, platform_var, component_var) ]
  names(data) <- c("date", "platform", "component")
  
  data$count <- "count" # set up a counting variable
  
  data <- reshape2::dcast(data, date + platform + component ~ count,
                          value.var = "count",
                          fun.aggregate = length)
  
  # Do some basic cleanup to prevent errors and such
  data$platform[ is.na(data$platform) ] <- "unknown"
  data$component[ is.na(data$component) ] <- "unknown"
  data <- data[ ! is.na(data$date) , ]
  
  # split by platform and component for first round of counts
  data <- by(data, INDICES = data$platform, function(x) x)
  
  data <- parallel::mclapply(data, function(y){
    
    components <- by(y, INDICES = y$component, function(x){
      
      drange <- sort(unique(x$date))
      
      result <- sapply(drange, function(date){
        sum(x$count[ x$date %in% seq(date - window - 1, date, by = "day") ], na.rm = T)
      })
      
      result <- data.frame(date = drange,
                           platform = rep(x$platform[ 1 ], length(drange)),
                           component = rep(x$component[ 1 ], length(drange)),
                           count = result,
                           stringsAsFactors = F)
      result
    })
    components <- recursive_rbind(components)
    
    components
  }, mc.cores = cpus)
  
  data <- recursive_rbind(data)
  
  # Get platform totals
  platforms <- reshape2::dcast(data, date ~ platform, 
                               value.var = "count",
                               fun.aggregate = function(x) sum(x, na.rm = T))
  
  platforms <- reshape2::melt(platforms, id.vars = c("date"),
                              variable.name = "platform", 
                              value.name = "platform_tot")
  
  platforms <- platforms[ platforms$platform_tot > 0 , ]
  
  platforms$platform <- as.character(platforms$platform)

  # Get component totals
  components <- reshape2::dcast(data, date ~ component, 
                               value.var = "count",
                               fun.aggregate = function(x) sum(x, na.rm = T))
  
  components <- reshape2::melt(components, id.vars = c("date"),
                              variable.name = "component", 
                              value.name = "component_tot")
  
  components <- components[ components$component_tot > 0 , ]
  
  components$component <- as.character(components$component) 
  
  # Get overall totals
  total <- reshape2::dcast(data, date ~ ., value.var = "count", 
                           fun.aggregate = function(x) sum(x, na.rm = T))
  names(total)[ 2 ] <- "total"
  
  # merge it all together
  result <- merge(data, platforms, all = T)
  result <- merge(result, components, all = T)
  result <- merge(result, total, all = T)
  
  # final cleanup and return result
  result <- result[ , c("date", "platform", "component", "count",
                        "platform_tot", "component_tot", "total") ]
  
  result
  
}


CalcTStatistic <- function(n1, N1, n2, N2, scale = NULL, cohend = FALSE){
  
  p1 <- n1 / N1
  p2 <- n2 / N2
  
  s2_1 <- p1 * (1 - p1)
  s2_2 <- p2 * (1 - p2)
  
  if (! is.null(scale) & cohend) 
    warning("scale is not NULL and cohend is TRUE. 
            This will create a scaled version of Cohen's d statistic 
            (which won't change your results much). 
            Did you mean to do this?")
  
  if (! is.null(scale)){
    if (! is.numeric(scale))
      stop("If scale is non-NULL, it must be numeric and preferrably an integer.")
    
    scale_f <- N1 / N2
    N2[ N2 > scale ] <- scale
    N1 <- scale_f * scale
  }
  
  if (cohend) {
    t <- (p1 - p2) / sqrt(((N1 - 1) * s2_1 + (N2 - 1) * s2_2) / (N1 + N2))
  } else {
    t <- (p1 - p2) / sqrt(s2_1 / N1 + s2_2 / N2)
  }
  t
}
