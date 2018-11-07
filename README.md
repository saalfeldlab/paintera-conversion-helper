# Paintera Conversion Helper
Script to assist conversion of n5 datasets to paintera-friendly formats, as specified [here](https://github.com/saalfeldlab/paintera/issues/61).

## Installation
paintera-conversion-helper is available on conda on the `hanslovsky` channel:
```
conda install -c hanslovsky jrun paintera-conversion-helper
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
or for a locally compiled fat jar:
```
java -Dspark.master=local[*] -jar target/paintera-conversion-helper-0.0.1-SNAPSHOT.jar [...]
```
with any desired command line arguments.

### Command Line Arguments
* `-d` or `--dataset` specifies a dataset to convert; uses the following syntax: `<n5 root>,<path
/to/dataset>,<raw|label>[,optional name]`. The optional name is only necessary if converting
multiple datasets of the same type (raw or label). **Note that this format is comma delimited**.
* `-o` or `--outputN5` specifies the N5 root to output to.
* `-b` or `--blocksize` **OPTIONALLY** specify a global block size to use for the output (comma
delimited or a single number for isotropic size). Defaults to `64,64,64`.
* `-s` or `--scale` **OPTIONALLY** specify multiple resolutions to downscale to.
If present, produces the requested downsampling, creating multiscale groups, and storing
datasets in `s0`, `s1`, ... `sN`, [conforming with formats 3 and 2](https://github.com/saalfeldlab/paintera/issues/61).
Each scale builds upon the previous one (e.g. `2,2,1 2,2,2` first downscales by 2 in x,y for `s1`, then downscales
*again* by 2 in x,y,z for `s2`), otherwise it simply stores a dataset in `s0`.
* `-r` or `--revert` specifies whether to revert the array attributes, such as when converting
data stored in (z,y,x) order, which will become (x,y,z) if the `revert` flag is present.
* `--winner-takes-all-downsampling` specifies whether to use "winner-take-all" downsampling for
integer-type labels. If this flag is present, it will be assumed that all input label datasets
are integer/long format (**Not** LabelMultisetType), and "winnter-take-all" downsampling will be used.
* `-c` or `--convert-entire-container` specifies whether to simply convert an entire container, and
the path to that container. If present, all `dataset` options will be ignored, and the datasets
inside of the provided container will be autodetected as *label* or *raw* data
(see [#1](https://github.com/saalfeldlab/paintera-conversion-helper/issues/1) for details)
and converted accordingly.
* `-h` or `--help` shows a less informative version of this explanation.

## Example Usage
```
java -Dspark.master=local[*] -jar target/paintera-conversion-helper-0.0.1-SNAPSHOT.jar \
     --dataset /path/to/root.n5,/path/to/raw,raw  \
     --dataset /path/to/root.n5,/path/to/labels,label \
     --outputN5 /path/to/output-root.n5 \
     --blocksize 64,64,64 \
     --scale 2,2,1 3,3,2 2,2,1
```

```
java -Dspark.master=local[*] -jar target/paintera-conversion-helper-0.0.1-SNAPSHOT.jar \
     --convert-entire-container /path/to/container.hdf \
     --outputN5 /path/to/output-root.n5 \
     --scale 2,2,1 2,2,2 3,3,2 \
     --revert
```

Note that HDF5 and N5 containers are supported as inputs for all operations. 
TODO: Add information on using the install script and change example usage accordingly.

