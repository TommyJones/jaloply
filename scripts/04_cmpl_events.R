################################################################################
# This script detects events from the cmpl_tstat data
################################################################################

rm(list = ls())

source("scripts/00_common_functions.R")

load("data_derived/cmpl_tstat.RData")

# remove any rows where pdstat is less than 0.75 or count is less than 4
cmpl_events <- cmpl_tstat[ cmpl_tstat$pdstat >= 0.75 & cmpl_tstat$count > 3, ]

# partition by platform and then component
cmpl_events <- by(cmpl_events, INDICES = cmpl_events$platform, function(x) x)

cmpl_events <- parallel::mclapply(cmpl_events, function(x){
  by(x, INDICES = x$component, function(y) y[ order(y$date) , ])
}, mc.cores = 4)

# count number of periods for each event
period_count <- parallel::mclapply(cmpl_events, function(x){
  sapply(x, nrow)
}, mc.cores = 4)

period_count <- unlist(period_count)

### Flag multiple issue/non-consecutive issue cases ----------------------------
# rules to retain an event:
# 1. at least 5 periods
# 2. split events where there is more than a 12-month period between
# 3. remove those where 

# remove those elements where less than 5 periods
cmpl_events <- parallel::mclapply(cmpl_events, function(x){
  result <- lapply(x, function(y){
    if(nrow(y) < 5)
      return(NULL)
    
    y
  })
  result[ ! sapply(result, is.null) ]
}, mc.cores = 4)

# table to get month differences
mid <- data.frame(date = sort(unique(cmpl_tstat$date)),
                  mid = 1:length(unique(cmpl_tstat$date)),
                  stringsAsFactors = FALSE)

# function to split issues
SplitIssues <- function(y, mid){
  
  # get integers pertaining to dates in y
  checkdiff <- mid[ mid$date %in% y$date , ]
  
  # diff returns difference, in months, between rows
  checkdiff$check <- c(1, diff(checkdiff$mid))
  
  # we have a "break" between events if there is more than 12 months between
  breakpoint_dates <- checkdiff$date[ checkdiff$check > 12 ]
  
  # split the result into separate issues based on breakpoints
  if (length(breakpoint_dates) == 0) { # if only one event...
    
    return(list(`1` = y))
    
    
  } else if (length(breakpoint_dates) == 1) { # if we have two events...
    split_issues <- list(`1` = y[ y$date < breakpoint_dates , ],
                         `2` = y[ y$date >= breakpoint_dates , ])
    
    # drop if any that remain have fewer than 5 months over the threshold
    split_issues <- split_issues[ sapply(split_issues, nrow) >= 5 ]
    
    names(split_issues) <- seq_along(split_issues)
    
    split_issues
    
  } else { # if we have more than two events...
    
    split_issues <- vector(mode = "list", length = length(breakpoint_dates) + 1)
    
    split_issues[[ 1 ]] <- y[ y$date < breakpoint_dates[ 1 ] , ]
    
    for(j in 2:length(breakpoint_dates)) {
      split_issues[[ j ]] <- y[ y$date < breakpoint_dates[ j ] &
                                  y$date >= breakpoint_dates[ j - 1 ], ]
    }
    
    split_issues[[ length(split_issues) ]] <- 
      y[ y$date >= breakpoint_dates[ length(breakpoint_dates) ] , ]
    
    # drop if any that remain have fewer than 5 months over the threshold
    split_issues <- split_issues[ sapply(split_issues, nrow) >= 5 ]
    
    names(split_issues) <- seq_along(split_issues)
    
    split_issues
  }
}

# apply function to each element and restructure list
cmpl_events <- parallel::mclapply(cmpl_events, function(x){
  result <- lapply(x, SplitIssues, mid = mid)
  result <- do.call(c, result)
  result
}, mc.cores = 4)

cmpl_events <- do.call(c, cmpl_events)




### Chevy corvette example -------------------------
# I have a lot to work out, so this is mostly experimentation

corvette <- cmpl_events[ grepl("CHEVROLET_CORVETTE", names(cmpl_events)) ]

names(corvette)

# so inefficient!!!
corvette <- lapply(corvette, function(x){
  dates <- seq(min(x$date) - 364, max(x$date), by = "day")
  
  cmpl_formatted[ cmpl_formatted$platform %in% x$platform &  
                    cmpl_formatted$component %in% x$component &
                    cmpl_formatted$datea %in% dates , ]
  
})

# which model years matter?
corvette_my <- lapply(corvette, function(x){
  my_freq <- table(x$vin_modelyear)
  my_freq <- my_freq / sum(my_freq, na.rm = T)
  
  cs <- cumsum(sort(my_freq, decreasing = TRUE))
  
  if(max(my_freq) >= 0.8)
    return(sort(names(my_freq)[ my_freq > 0.15 ]))
  
  result <- intersect(names(cs)[ cs < 0.8 ], 
                      names(my_freq)[ my_freq > 0.05 ])
  
  sort(result)
})

# remove irrelevant model years
corvette <- mapply(FUN = function(data, my) data[ data$vin_modelyear %in% my , ],
                   data = corvette, my = corvette_my, 
                   SIMPLIFY = FALSE)

corvette_summaries <- lapply(corvette, function(x){
  Summarize(docs = x$cdescr)
})
