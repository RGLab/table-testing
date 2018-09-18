FROM bioconductor/devel_core2:latest

RUN R -e 'BiocManager::install(c("devtools","HDF5Array", "bigmemoryExtras", "optparse", "ff"))\
           ;devtools::install_github("RGLab/mbenchmark")\
            '

COPY convert.R /convert.R
COPY converters.R /converters.R


ENTRYPOINT ["/convert.R"]
