################################################################################
# This script defines some common functions for use in this project
################################################################################

### Stick libraries used here --------------------------------------------------
library(stringr)
library(lubridate)
library(reshape2)
library(httr)

### Functions go here. Put a description in the comments -----------------------

recursive_rbind <- function(list_object){
  # this function takes a list and performs rbind in a recursive way
  # recursive makes it faster and more memory efficient

  if(length(list_object) <= 1000)
    return(do.call(rbind, list_object))

  batches <- seq(1, length(list_object), by = 1000)

  list_object <- lapply(batches, function(x){
    do.call(rbind, list_object[ x:(min(x + 999, length(list_object))) ])
  })

  recursive_rbind(list_object)
}

CalcRollingCount <- function(data, date_var, platform_var, component_var,
                             window = 365, cpus = 4){
  # This function calculates counts over a rolling window. The outputs of this 
  # function feed into the CalcTStatistic function. One thing to keep in mind,
  # you need to make sure you don't have any missing dates (if you care) before
  # putting into this function.
  
  
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
  # This function calculates t-statistics and cohen's d-statistics
  # The scale option will allow you to cap the maximum number of observations
  # to consider in the (much likely larger) control group (N2)
  
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