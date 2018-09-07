suppressPackageStartupMessages(library(bigmemory))
suppressPackageStartupMessages(library(HDF5Array))#must load it first to avoid namespace conflicting
suppressPackageStartupMessages(library(DelayedArray))
suppressPackageStartupMessages(library(mbenchmark))
suppressPackageStartupMessages(library(rhdf5))
suppressPackageStartupMessages(library(ff))


h5_to_delayedarray <- function(path, dataset_name = "data", verbose = FALSE){
  #load h5
  hm = HDF5Array(path, dataset_name)
  dims <- dim(hm)
  if(verbose)
    message("HDF5Array: ", paste(dims, collapse = "x"))
  hm
}

bigmemory_to_delayedarray <- function(path, dataset_name = NULL, verbose = FALSE){
  bm <- attach.big.matrix(path)
  bmseed <- BMArraySeed(bm)
  
  bm <- DelayedArray(bmseed)
  dims <- dim(hm)
  if(verbose)
    message("BMArray: ", paste(dims, collapse = "x"))
  bm
}

ff_to_delayedarray <- function(path, dataset_name = NULL, verbose = FALSE){
  meta <- read.csv(file.path(path, "fm_meta.csv"), stringsAsFactors = FALSE)
  fm <- ff(vmode=meta$vmode, dim=as.integer(strsplit(split = ",", meta$dim)[[1]]), filename = meta$ff.file)
  
  fm <- DelayedArray(fm)
  dims <- dim(hm)
  if(verbose)
    message("FMArray: ", paste(dims, collapse = "x"))
  fm
}