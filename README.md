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
format. For local spark usage, run for a conda install:
```
paintera-conversion-helper [...]
```

### Usage Example
To convert the `raw` and `neuron_ids` datasets of [sample A of the cremi challenge](https://cremi.org/data/) into Paintera format with mipmaps on Linux, assuming that you downloaded the data into `$HOME/Downloads`, run:
```sh
paintera-conversion-helper \
    -r \
    -d $HOME/Downloads/sample_A_padded_20160501.hdf,volumes/labels/neuron_ids,label \
    -d $HOME/Downloads/sample_A_padded_20160501.hdf,volumes/raw,raw \
    -o $HOME/Downloads/sample_A_padded_20160501.n5 \
    -b 64,64,64 \
    -s 2,2,1 2,2,1 2,2,1 2,2,2 2,2,2 2,2,2 \
    -m -1 -1 4 2 \
    --label-block-lookup-backend-n5=10000
```

or for a locally compiled fat jar:
```
java -Dspark.master=local[*] -jar target/paintera-conversion-helper-0.4.1-SNAPSHOT-shaded.jar [...]
```
with any desired command line arguments.

### Usage Help
```
$ paintera-conversion-helper --help
Usage: <main class> [-hr] [--winner-takes-all-downsampling]
                    [--label-block-lookup-backend-n5=BLOCK_SIZE]
                    [-c=<convertEntireContainer>] -o=<outputN5>
                    [--offset=<offset>[,<offset>...]]...
                    [--resolution=<resolution>[,<resolution>...]]...
                    [-b=BLOCK_SIZE[,BLOCK_SIZE...]]... [-d=<datasets>]...
                    [--downsample-block-sizes=<downsampleBlockSizes>...]...
                    [-m=<maxNumEntries>...]... [-s=<scales>...]...
      --downsample-block-sizes=<downsampleBlockSizes>...
                             Block size for each downscaled level in the format bx,
                               by,bz or b for isotropic block size. Does not need to
                               be specified for each scale level (defaults to
                               previous level if not specified, or BLOCK_SIZE if not
                               specified at all)
      --label-block-lookup-backend-n5=BLOCK_SIZE
                             Use n5 as backend for label block lookup with specified
                               BLOCK_SIZE.
      --offset=<offset>[,<offset>...]
                             Offset in world coordinates.
      --resolution=<resolution>[,<resolution>...]
                             Voxel size.
      --winner-takes-all-downsampling
                             Use winner-takes-all-downsampling for labels
  -b, --blocksize=BLOCK_SIZE[,BLOCK_SIZE...]
                             block size for initial conversion in the format bx,by,
                               bz or b for isotropic block size. Defaults to 64,64,64
  -c, --convert-entire-container=<convertEntireContainer>
                             Convert entire container; auto-detect dataset types
  -d, --dataset=<datasets>   Comma delimited description of dataset; <n5 root path>,
                               <path/to/dataset>,<raw|label|channel[:
                               channelAxis=<axis>][,optional name]
  -h, --help                 display a help message
  -m, --max-num-entries=<maxNumEntries>...
                             max number of entries for each label multiset at each
                               scale. Pick lower number for higher scale levels
  -o, --outputN5=<outputN5>
  -r, --revert               Reverts array attributes
  -s, --scale=<scales>...    Factor by which to downscale the input image. Factors
                               are relative to the previous level, not to level
                               zero. Format either fx,fy,fz or f
```

