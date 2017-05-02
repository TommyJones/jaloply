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
  
  # Make sure we have full coverage of dates, platforms, and components
  # A platform won't have any entries before its first entry in the database
  # (This approxmiates a platform not being manufactured)
  date_fill <- data.frame(date = sort(unique(data$date)),
                          stringsAsFactors = F)
  
  # # for now (4/27/2017) component_fill and fill_mat are unused
  # component_fill <- data.frame(component = sort(unique(data$component)),
  #                              stringsAsFactors = F)
  # 
  # fill_mat <- merge(date_fill, component_fill, all = T)
  
  # split by platform and component for first round of counts
  data <- by(data, INDICES = data$platform, function(x) x)
  
  data <- parallel::mclapply(data, function(y){
   
    drange <- sort(unique(date_fill$date[ date_fill$date >= min(y$date) ])) # calls "global" date_fill
    
    components <- by(y, INDICES = y$component, function(x){
      
      # x <- merge(x, date_fill, all = T) # calls "global" date_fill
      
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
  
  # replace any missing values with zero
  result$count[ is.na(result$count) ] <- 0
  result$platform_tot[ is.na(result$platform_tot) ] <- 0
  result$component_tot[ is.na(result$component_tot) ] <- 0
  result$total[ is.na(result$total) ] <- 0
  
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


Summarize <- function(docs){
  # this is the working document summarization function
  # it takes a vector of documents as input and returns a vector of sentences
  # as output. 
  
  # parse sentences
  split <- stringi::stri_split_boundaries(docs, type = "sentence")
  split <- unlist(split)
  names(split) <- 1:length(split)
  
  # make dtm with stems
  dtm <- textmineR::CreateDtm(split, ngram_window = c(1,2),
                              stem_lemma_function = function(x) SnowballC::wordStem(x, "porter"))
  
  # make adjacency matrix
  adj <- dtm / sqrt(sum(dtm * dtm))
  
  adj <- adj %*% Matrix::t(adj)
  
  adj <- igraph::graph.adjacency(adj, mode = "undirected", weighted=TRUE, diag = FALSE)
  
  # top N sentences (keywords) based on eigenvector centrality
  top_n <- igraph::evcent(graph = adj)
  
  top_n <- top_n$vector[ top_n$vector >= 0.75 ]
  
  split[ names(top_n)[ order(top_n, decreasing = T) ] ]
  
}

