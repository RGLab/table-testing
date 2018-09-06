suppressPackageStartupMessages(library(bigmemory))
suppressPackageStartupMessages(library(HDF5Array))#must load it first to avoid namespace conflicting
suppressPackageStartupMessages(library(DelayedArray))
suppressPackageStartupMessages(library(mbenchmark))
suppressPackageStartupMessages(library(rhdf5))
suppressPackageStartupMessages(library(ff))


h5_to_bigmemory <- function(h5file, dataset_name = "data", output_path, nblocks = 100, verbose = FALSE){
  #load h5
  hm = HDF5Array(h5file, dataset_name)
  dims <- dim(hm)
  if(verbose)
    message("source data dimension: ", paste(dims, collapse = "x"))
 
  bm <- big.matrix(nrow = dims[1], ncol = dims[2], backingpath = output_path, backingfile = "bm", descriptorfile = "bm.desc")
 
  i <- 1
  while(i < dims[2])
  {
    j <- i + nblocks - 1
    if(j > dims[2])
      j <- dims[2]
    bm[, i:j] <- as.matrix(hm[, i:j])
    i <- j + 1
    if(verbose)
      message("writing ",i, "th column ...")
  }
  if(!all.equal(as.matrix(hm[,1:3]), as.matrix(bm[,1:3])))
    stop("The destination data value'", output_path, "' is not consistent with the source data '", h5file, "'")
}

h5_to_ff <- function(h5file, dataset_name = "data", output_path, nblocks = 100, verbose = FALSE){
  #load h5
  hm = HDF5Array(h5file, dataset_name)
  dims <- dim(hm)
  if(verbose)
    message("source data dimension: ", paste(dims, collapse = "x"))
  
  
  ff.file <- file.path(output_path, "fm")
  fm <- ff(vmode="double", dim=dims, filename = ff.file)
  i <- 1
  while(i < dims[2])
  {
    j <- i + nblocks - 1
    if(j > dims[2])
      j <- dims[2]
    fm[, i:j] <- as.matrix(hm[, i:j])
    i <- j + 1
    if(verbose)
      message("writing ", i, "th column ...")
  }
  if(!all.equal(as.matrix(hm[,1:3]), as.matrix(fm[,1:3])))
    stop("The destination data value'", output_path, "' is not consistent with the source data '", h5file, "'")
}
