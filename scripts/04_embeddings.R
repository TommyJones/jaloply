################################################################################
# This script creates word embeddings of the NHTSA data
################################################################################

rm(list = ls())

library(textmineR)

### Load all of the textual data ----
load("data_derived/cmpl_raw.RData")
load("data_derived/inv_raw.RData")
load("data_derived/rcl_raw.RData")
load("data_derived/tsbs_raw.RData")

### Combine into a single character vector ----

docs <- c(cmpl$cdescr,
          inv$summary,
          inv$subject,
          rcl$desc_defect,
          rcl$conequence_defect,
          rcl$corrective_action,
          rcl$notes,
          tsbs$summary)

# docs <- cmpl$cdescr

# keep only unique documents
docs <- docs[ ! duplicated(docs) & ! docs %in% c("", " ") ]

### Get a vocabulary list ----
# dtm <- CreateDtm(doc_vec = docs, stopword_vec = c())
# 
# tf <- TermDocFreq(dtm)
# 
# stopwords <- tf$term[ tf$term_freq < 5 ]
# 
# keep <- tf$term[ tf$doc_freq > 4 ]

### Create a TCM ----

# doing this in batches to conserve memory
batches <- seq(1, length(docs), by = 5000)

batches <- lapply(batches, function(x) docs[ x:min(x + 4999, length(docs)) ])

tcm <- lapply(batches, function(x){
  out <- CreateTcm(doc_vec = x, 
                   skipgram_window = 3,
                   stopword_vec = c())
  
  # v <- intersect(colnames(out), keep)
  # 
  # out <- out[ v , v ]
})

# Make sure each submatrix has all words in it
MakeBigger <- function(tcm, vocab) {
  
  add <- Matrix(0, nrow = nrow(tcm),
                ncol = length(setdiff(vocab, colnames(tcm))),
                sparse = TRUE)
  
  colnames(add) <- setdiff(vocab, colnames(tcm))
  rownames(add) <- rownames(rownames(tcm))
  
  out <- cBind(tcm, add)
  
  add <- rBind(add,
               Matrix(0, nrow = ncol(out) - nrow(out),
                      ncol = ncol(out) - nrow(out),
                      sparse = TRUE))
  
  rownames(add) <- colnames(out)
  
  out <- rBind(t(add), out)
  
  out[ vocab , vocab ]
}

vocab <- sort(unique(unlist(lapply(tcm, colnames))))

tcm <- parallel::mclapply(tcm, function(x) MakeBigger(x, vocab), mc.cores = 4)

batches <- seq(1, length(tcm), by = 10)

tcm <- lapply(batches, function(x) tcm[ x:min(x + 9, length(tcm)) ])

tcm <- parallel::mclapply(tcm, function(x) Reduce("+", x), mc.cores = 4)

tcm <- Reduce("+", tcm)

### remove very infrequent or very frequent terms ---

# get rid of stop words
stopwords <- tidytext::stop_words$word

v <- setdiff(rownames(tcm), stopwords)

tcm <- tcm[ v , v ]

# get rid of infrequent words
keep <- rowSums(tcm) > 15

tcm <- tcm[ keep , keep ]

### Save TCM so we don't have to re-cacluate ---
save(tcm, file = "data_derived/tcm.RData")

### Fit a big ol' list of embeddings ----

embeddings <- FitLdaModel(dtm = tcm, k = 500, iterations = 800)

# get gamma for predictions
embeddings$gamma <- CalcPhiPrime(embeddings$phi,
                                 embeddings$theta)

# coherence
embeddings$coherence <- CalcProbCoherence(embeddings$phi, tcm)

# save so we don't have to re-calculate
save(embeddings, file = "data_derived/embeddings_raw.RData")

gc()



