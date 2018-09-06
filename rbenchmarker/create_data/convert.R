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
source("/scripts/converters.R")
# specify our desired options in a list
# by default OptionParser will add an help option equivalent to 
# make_option(c("-h", "--help"), action="store_true", default=FALSE, 
#               help="Show this help message and exit"))
option_list <- list(
  make_option(c("-v", "--verbose"), action="store_true", default=TRUE,help="Print extra output [default \"%default\"]"),
  make_option(c("-q", "--quietly"), action="store_false",dest="verbose", help="Print little output"),
  make_option("--from", default= "h5",help="the source data format [default \"%default\"]"),
  make_option("--dataset_name", default= "data",help="the dataset name in the source data format(only applicable for h5) [default %default]"),
  
  make_option("--to", default= "bigmemory", help = "the destination data format [default \"%default\"]"),
  make_option("--src_path",help="the source data file path"),
  make_option("--dest_path",help="the destination data file path"),
  make_option("--nblocks", default= 100, help = "the unit size of block processing [default \"%default\"]")
  
)

# get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults,
opt <- parse_args(OptionParser(option_list=option_list))

if(is.null(opt$src_path))
  stop("Missing argument --src_path")
if(is.null(opt$dest_path))
  stop("Missing argument --dest_path")

thisCall <- paste0(opt$from, "_to_", opt$to)
if(!existsFunction(thisCall)) {
  stop("converter function: '", thisCall, "' not found!") 
} else {
  message("Run converter: '", thisCall, "' ...") 
  
  do.call(thisCall, list(opt$src_path, opt$dataset_name, opt$dest_path, opt$nblocks, opt$verbose))
}

