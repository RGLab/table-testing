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
suppressPackageStartupMessages(library(yaml))

source("converters.R")

# specify our desired options in a list
# by default OptionParser will add an help option equivalent to
# make_option(c("-h", "--help"), action="store_true", default=FALSE,
#               help="Show this help message and exit"))
option_list <- list(
  make_option(c("-v", "--verbose"), action="store_true", default=TRUE,help="Print extra output [default \"%default\"]"),
  make_option(c("-q", "--quietly"), action="store_false",dest="verbose", help="Print little output"),
  make_option("--data-yaml",help="the yaml file describe the test data"),
  make_option("--output-path",help="the destination data file path"),
  make_option("--nblocks", default= 100, help = "the unit size of block processing [default \"%default\"]")

)

# get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults,
opt <- parse_args(OptionParser(option_list=option_list))
output <- opt[["output-path"]]
if(is.null(output))
  stop("Missing argument --output-path")


data.list <- read_yaml(opt[["data-yaml"]])
src.list <- data.list[["sources"]]
dest.list <- data.list[["outputs"]]
for(src in names(src.list))
{
  message("data source: ", src, " ...")
  src <- src.list[[src]]
  for(dest in names(dest.list))
  {
    message("converting to: ", dest)
    dest <- dest.list[[dest]]
    thisCall <- paste0(src[["type"]], "_to_", dest[["format"]])
    if(!existsFunction(thisCall)) {
      stop("converter function: '", thisCall, "' not found!")
    } else {

      do.call(thisCall, list(src[["path"]]
                             , dataset_name = src[["args"]][["dataset_name"]]
                             , output_path = output
                             , opt$nblocks, opt$verbose))
    }

  }

}



