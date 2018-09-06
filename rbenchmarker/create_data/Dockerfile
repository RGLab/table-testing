FROM bioconductor/devel_core2

RUN R -e 'BiocManager::install(c("devtools","HDF5Array", "bigmemoryExtras", "optparse", "ff"))\
           ;devtools::install_github("RGLab/mbenchmark")\
            '

COPY convert.R /scripts/convert.R
COPY converters.R /scripts/converters.R


ENTRYPOINT ["/scripts/convert.R"]
