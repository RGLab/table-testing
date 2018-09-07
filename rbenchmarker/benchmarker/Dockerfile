FROM bioconductor/devel_core2

RUN R -e 'BiocManager::install(c("devtools","HDF5Array", "bigmemoryExtras", "optparse", "ff"))\
           ;devtools::install_github("RGLab/mbenchmark")\
            '

COPY file_to_delayedarray.R /file_to_delayedarray.R
COPY run_benchmark.R /run_benchmark.R


ENTRYPOINT ["/run_benchmark.R"]
