[![](https://travis-ci.org/saalfeldlab/paintera-conversion-helper.svg?branch=master)](https://travis-ci.org/saalfeldlab/paintera-conversion-helper)

# Paintera Conversion Helper
Script to assist conversion of n5 datasets to paintera-friendly formats, as specified [here](https://github.com/saalfeldlab/paintera/issues/61).

## Installation
paintera-conversion-helper is available on conda on the `hanslovsky` channel:
```
conda install -cconda-forge -c hanslovsky paintera-conversion-helper
```
If necessary, you can install `openjdk` and `maven` from `conda-forge`:
```
conda install -c conda-forge maven openjdk
```

Alternatively, `paintera-conversion-helper` can be installed from PyPI through pip:
```
pip install paintera-conversion-helper
```


## Compile
To compile the conversion helper into a jar, simply run
```
mvn -Denforcer.skip=true clean package
```

To run locally build a fat jar including Spark:
```
mvn -Denforcer.skip=true -PfatWithSpark clean package
```

To run on the Janelia cluster build a fat jar without Spark:
```
mvn -Denforcer.skip=true -Pfat clean package
```

## Running
This conversion tool currently supports any number of datasets (raw or label) with a
single (global) block size, and will output to a single N5 group in a paintera-compatible
format. For local spark usage, [install throguh conda or pip](#installation), and run:
```
paintera-convert to-paintera [...]
```

Introduced in version `0.7.0`, the
```
paintera-convert extract-to-scalar [...]
```
extracts the highest resolution scale level of a Paintera dataset as a scalar `uint64`. Dataset. This is useful for using Paintera painted labels (and assignments) in downstream processing, e.g. classifier training. Optionally, the `fragment-segment-assignment` can be considered and additional assignments can be added (versions `0.8.0` and later). See `extract-to-scalar --help` for more details.


### Janelia cluster

Clone the repository with submodules:
```
git clone --recursive https://github.com/saalfeldlab/paintera-conversion-helper.git
```
If you have already cloned the repository, run this after cloning to fetch the submodules:
```
git submodule update --init --recursive
```

Then, run the following script to build the package:
```
./build-for-cluster.py
```

For submitting a job to the Janelia cluster you can use the following script:
```
startup-scripts/spark-janelia/convert.py  <number of cluster nodes>  <other parameters>
```
The first parameter is the number of cluster nodes to use (for example, 5), and the rest is the same parameters as in the `paintera-convert` command.


### Usage Example
To convert the `raw` and `neuron_ids` datasets of [sample A of the cremi challenge](https://cremi.org/data/) into Paintera format with mipmaps on Linux, assuming that you downloaded the data into `$HOME/Downloads`, run:
```sh
paintera-convert to-paintera \
  --scale 2,2,1 2,2,1 2,2,1 2 2 \
  --revert-array-attributes \
  --output-container=paintera-converted.n5 \
  --container=sample_A_20160501.hdf \
    -d volumes/raw \
      --target-dataset=volumes/raw2 \
      --dataset-scale 3,3,1 3,3,1 2 2 \
      --dataset-resolution 4,4,40.0 \
    -d volumes/labels/neuron_ids
```

or for a locally compiled fat jar:
```
java -Dspark.master=local[*] -jar target/paintera-conversion-helper-0.4.1-SNAPSHOT-shaded.jar [...]
```
with any desired command line arguments.

### Usage Help
```
$ paintera-convert to-paintera --help
Usage: paintera-convert to-paintera [[--block-size=X,Y,Z|U] [--scale=X,Y,Z|U...] [--scale=X,Y,Z|U...]...
                                    [--downsample-block-sizes=X,Y,Z|U...] [--downsample-block-sizes=X,Y,Z|U...]...
                                    [--revert-array-attributes] [--resolution=X,Y,Z|U] [--offset=X,Y,Z|U] [-m=N...]
                                    [-m=N...]... [--label-block-lookup-n5-block-size=N]
                                    [--winner-takes-all-downsampling]] ([--container=CONTAINER] [] (-d=DATASET
                                    [--target-dataset=TARGET_DATASET] [[--type=TYPE]   ])...)... [--overwrite-existing]
                                    [--help] --output-container=OUTPUT_CONTAINER [--spark-master=<sparkMaster>]
                                    

Options:

      --block-size=X,Y,Z|U   Use --container-block-size and --dataset-block-size for container and dataset specific
                               block sizes, respectively.
      --scale=X,Y,Z|U...     Relative downsampling factors for each level in the format x,y,z, where x,y,z are
                               integers. Single integers u are interpreted as u,u,u.
                             Use --container-scale and --dataset-scale for container and dataset specific scales,
                               respectively.
      --downsample-block-sizes=X,Y,Z|U...
                             Use --container-downsample-block-sizes and --dataset-downsample-block-sizes for container
                               and dataset specific block sizes, respectively.
      --revert-array-attributes
                             Revert array attributes like resolution and offset, i.e. [x, y, z] -> [z, y, x].
                             Use --container-revert-array-attributes and --dataset-revert-array-attributes for
                               container and dataset specific setting, respectively.
      --resolution=X,Y,Z|U   Specify resolution (overrides attributes of input datasets, if any).
                             Use --container-resolution and --dataset-resolution for container and dataset specific
                               resolution, respectively.
      --offset=X,Y,Z|U       Specify offset (overrides attributes of input datasets, if any).
                             Use --container-offset and --dataset-offset for container and dataset specific resolution,
                               respectively.
  -m, --max-num-entries=N... Limit number of entries for non-scalar label types by N. If N is negative, do not limit
                               number of entries.  If fewer values than the number of down-sampling layers are
                               provided, the missing values are copied from the last available entry.  If none are
                               provided, default to -1 for all levels.
                             Use --container-max-num-entries and --dataset-max-num-entries for container and dataset
                               specific settings, respectively.
      --label-block-lookup-n5-block-size=N
                             Set the block size for the N5 container for the label-block-lookup.
                             Use --container-label-block-lookup-n5-block-size and
                               --dataset-label-block-lookup-n5-block-size for container and dataset specific settings,
                               respectively.
      --winner-takes-all-downsampling
                             Use scalar label type with winner-takes-all downsampling.
                             Use --container-winner-takes-all-downsampling and --dataset-winner-takes-all-downsampling
                               for container and dataset specific settings, respectively.
      --overwrite-existing
      --output-container=OUTPUT_CONTAINER

      --spark-master=<sparkMaster>

      --container=CONTAINER
  -d, --dataset=DATASET
      --target-dataset=TARGET_DATASET

      --type=TYPE
      --help
```

