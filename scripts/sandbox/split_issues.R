
### Flag multiple issue/non-consecutive issue cases ----------------------------

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
    
  } else { # if we have more than one event...
    
    split_issues <- vector(mode = "list", length = length(breakpoint_dates) + 1)
    
    for(j in seq_along(breakpoint_dates)){
      split_issues[[ j ]] <- y[ y$date < breakpoint_dates[ j ] , ]
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
