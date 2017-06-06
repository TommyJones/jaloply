Query <- function(make, model, model_year, list_obj, tstat_obj){
  
  r <- list_obj[ grepl(paste(toupper(make), toupper(model), sep = "_"), names(list_obj)) ]
  
  mycheck <- sapply(r, function(x) model_year %in% x$model_year)
  
  
  if(sum(mycheck) == 0){
    message("Congratulations! There don't appear to be any issues with this platform!")
    return()
  }
  
  r <- r[ mycheck ]
  
  message("There are ", length(r), " unique issues with the ", model_year, " ", make, " ", model, ".\n")
  
  components <- sort(unique(sapply(r, function(x) x$component)))
  
  message("These issues affect the following components: \n", paste(components, collapse = "\n"))
  
  for(j in seq_along(r)){
    
    readline(prompt = paste("Press [enter] to see ", r[[ j ]]$component, "\n"))
    
    plot(tstat_obj[ tstat_obj$platform == paste(toupper(make), toupper(model), sep = "_") & 
                      tstat_obj$component == r[[ j ]]$component , c("date", "pdstat") ],
         type = "l", lwd = 2, ylim = c(0, 1),
         main = paste0(r[[ j ]]$component, "\n", "Model Years: ", paste(r[[ j ]]$model_years, collapse = " ")),
         col = "white")
    
    abline(v = seq(min(r[[ j ]]$dates), max(r[[ j ]]$dates), by = "day"), col = rgb(1,0,0,0.2))
    
    lines(tstat_obj[ tstat_obj$platform == paste(toupper(make), toupper(model), sep = "_") & 
                        tstat_obj$component == r[[ j ]]$component , c("date", "pdstat") ],
           type = "l", lwd = 2, ylim = c(0, 1),
           main = paste0(r[[ j ]]$component, "\n", "Model Years: ", paste(r[[ j ]]$model_years, collapse = " ")))
    
    message("\n", r[[ j ]]$component, "\n")
    
    message("\nNumber of complaints: ", length(r[[ j ]]$narratives), "\n")
    
    message("\nTypical complaint:\n\n", r[[ j ]]$summaries[ 1 ], "\n\n")
    
    View(as.data.frame(r[[ j ]]$narratives, stringsAsFactors = FALSE))
    
  }
  
  return()
  
}
