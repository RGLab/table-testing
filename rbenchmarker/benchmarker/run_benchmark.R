#!/usr/bin/env Rscript
# Copyright 2010-2013 Trevor L Davis <trevor.l.davis@gmail.com>
# Copyright 2008 Allen Day
#
#  This file is free software: you may copy, redistribute and/or modify it
#  under the terms of the GNU General Public License as published by the
#  Free Software Foundation, either version 2 of the License, or (at your
#  option) any later version.
#
#  This file is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
suppressPackageStartupMessages(library("optparse"))
suppressPackageStartupMessages(library(bigmemory))
suppressPackageStartupMessages(library(HDF5Array))#must load it first to avoid namespace conflicting
suppressPackageStartupMessages(library(DelayedArray))
suppressPackageStartupMessages(library(mbenchmark))
suppressPackageStartupMessages(library(rhdf5))
suppressPackageStartupMessages(library(ff))
suppressPackageStartupMessages(library(yaml))

source("file_to_delayedarray.R")

#' load the different matrix format into a list of DelayedArray objects
#' @param yaml_path the path to a yaml file that describes the data path and format
load_data <- function(yaml_path, verbose = FALSE){
  data.src <- read_yaml(yaml_path)[[1]]
  sapply(data.src, function(src){
        sapply(src, function(dat){
              thisCall <- paste0(dat$format, "_to_delayedarray")
              if(!existsFunction(thisCall)) {
                stop("delayed array loader function: '", thisCall, "' not found!")
              } else {
                # message("delayed array loader function: '", thisCall, "' ...")

                do.call(thisCall, list(dat$path, dat[["args"]][["dataset_name"]], verbose))
              }

            }, simplify = FALSE)
      }, simplify = FALSE)
}
# specify our desired options in a list
# by default OptionParser will add an help option equivalent to
# make_option(c("-h", "--help"), action="store_true", default=FALSE,
#               help="Show this help message and exit"))
option_list <- list(
  make_option(c("-v", "--verbose"), action="store_true", default=TRUE,help="Print extra output [default %default]"),
  make_option("--test-yaml",help="the yaml file describe the test data"),
  make_option("--task", default= "subsetting"
                      ,help="the task to run, currently supported tasks: 'subsetting','traversing'. [default \'%default\']"),

  make_option("--times"
              , default= 5
              , help = "The repetitions to run [default %default]"),
  make_option("--max-percent-of-rows"
              , default= 0.1
              , help="(only applicable for 'subsetting' task) It is the percentage of the total number of rows of the data. used to set the upper bound of size of region to select.[default %default]"
                ),
  make_option("--nsubset", default= 5, help = "(only applicable for 'subsetting' task) the number of sub-regions to test. with the region size evenly increased up to the size limited by 'ubound' argument [default %default]"
              ),
 make_option("--shapes", default= "0.5, 1, 2", help="(only applicable for 'subsetting' task) vectors of col:row ratio, which defines the shape of the selected region [default \"%default\"]"
                                             ),
 make_option("--drop-page-cache", action="store_true", default=FALSE,help="whether to drop the page cache between iterations [default %default]"),
 make_option("--output-path",help="the path used for storing results")

)

# get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults,
opt <- parse_args(OptionParser(option_list=option_list))
yamlfile <- opt[["test-yaml"]]
if(is.null(yamlfile))
  stop("Missing argument --test-yaml")
output <- opt[["output-path"]]
if(is.null(output))
  stop("Missing argument --output-path")
shapes <- as.numeric(strsplit(split = ",", opt$shapes)[[1]])
# message(shapes)
mat.lists <- load_data(yamlfile, opt$verbose)
for(src in names(mat.lists))
{

  suppressWarnings(res <- mbenchmark(mat.lists[[src]]
                                  , type = opt$task
                                  , times = opt$times
                                  , ubound = opt[["max-percent-of-rows"]]
                                  , nsubset = opt$nsubset
                                  , shape = shapes
                                  , trace_mem = TRUE
                                  , clear_page_cache = opt[["drop-page-cache"]]
                                  , cache.file = file.path(output, paste0(src, "_", opt$task, ".csv"))
                                  , verbose = opt$verbose))
   png(file.path(output,  paste0(src, "_", opt$task, ".png")))
   p <-autoplot(res)
    if(opt$task == "subsetting")
      p <- p + scale_y_log10()
  dev.off()
  # plot_mem(res, units = "Kb") + scale_y_log10()



}