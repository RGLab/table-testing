# 1. Running the benchmark tasks

The rbenchmarker tasks can be launched by building the docker image defined and running it:

```bash
docker build -t benchmarker_r .
#Print the helps for the supported arguments
docker run benchmarker_r -h
#run the benchmarker leaving most arguments as default
docker run -v /path/to/data:/data -v /path/to/output:/output benchmarker_r  \
             --data-path=/data --output-path=/output - --max-percent-of-rows=0.01 --verbose
```
# 2. How it works

## 2.1 Tasks

The `rbenchmarker` tests the basic common operations for the on-disk matrix

-   subsetting
    -   `region_selection`: continuous block selection
    -   `random_slicing`: non-continuous slab selection
-   traversing
    -   `rowSums`
    -   `colSums`

The operation type is specified by the `--task` argument of the image entrypoint :

- `--task`: the task to run, currently supported tasks: 'subsetting','traversing'. Default is 'subsetting'.

## 2.2 The access patterns (only applicable for `subsetting` task)

The IO performance is significantly affected by the access pattern given the specific file format and its storage layout. Therefore it is important to measure the IO speed of various access patterns (e.g. size, shape) across different formats to access their overall performance. These are controlled by the following three arguments:

- `--max-percent-of-rows`:  the percentage of the total number of rows of the data. It is used to set the upper bound of size of region to select. Default is 0.1. To get a quick test run, try to set it to the smaller size (e.g. 0.001)
- `--nsubset`: the number of sub-matrix to test. Default value is 5. The size of sub-matrix is evenly incremented up to the size bounded by 'max-percent-of-rows' argument. 
-	`--shapes`: a string containing the comma seperated numeric values, which represents the ratio of col:row, i.e. the shape of the selected region. The default shapes are "0.5, 1, 2", which represents `long`, `square` and `wide` selection pattern.

## 2.3 Input and output

- `--data-path`: The path, within the docker container, to the sources data to test. It also expects the `test.yaml` file that defines the details of test data.
- `--output-path`: The path, again within the docker container, where the benchmark output should be written.
- `--verbose`: Print extra output.

The command above assumes the data and `test.yaml` file exists in `/path/to/data`, and it will place all of the results in subdirectories of `/path/to/output`.

- the timing details are stored in csv file 

- the ggplots stored in png files
Note that the `csv` files are also used as cache to enable the benchmarker to resume from the previous interrupted-run to save time.
Make sure to remove these files if the fresh run is intended.

## 2.4 Repitition

Once the access patterns and data sources are specified, the benchmarker will test each sub-task (the combination of subset sizes and shapes) against all the given data formats for each iteration. And the total number of iterations/repititions is specified by:

`--times`: The repetitions to run [default 5]

There are other optional arguments can be used to have the finer control on how and what task to be run.
type `-h` for details.


# 3. Adding new data sources

The data sources are defined in the `sources` section of [test.yaml](test.yaml). Each object has the following keys:

- path: The local path where the source data can be loaded
- format: The type of the source data. There has to be a function called `{format}_to_delayedarray` in [file_to_delayedarray.R](file_to_delayedarray.R) that can convert the source data into a DelayedArray data structure
- args: Any additional keyword arguments to pass to the `{format}_to_delayedarray` function

So adding a data source to testing requires three things:

1. Add an entry to [test.yaml](test.yaml)
2. Make sure there is an appropriate `{format}_to_delayedarray` in [file_to_delayedarray.R](file_to_delayedarray.R).
See[How to Implementing A DelayedArray Backend](http://bioconductor.org/packages/release/bioc/vignettes/DelayedArray/inst/doc/02-Implementing_a_backend.html)
3. Add any dependencies to the [Dockerfile](Dockerfile]
