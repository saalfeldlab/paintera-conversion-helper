# Paintera Conversion Helper
Script to assist conversion of n5 datasets to paintera-friendly formats, as specified [here](https://github.com/saalfeldlab/paintera/issues/61).

## Compile
Currently, requires master branch of [label-utilities-spark](https://github.com/saalfeldlab/label-utilities-spark).
After cloning, label-utilities-spark can be installed with
```
mvn clean install
```

Then, to compile the conversion helper into a jar, simply run
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
format. For local spark usage, run
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
* `-s` or `--scale` **OPTIONALLY** specify multiple resolutions to downscale to. If not present,
produces a single resolution for each dataset [conforming with formats 3 and 1](https://github.com/saalfeldlab/paintera/issues/61).
If present, produces the requested downsampling, creating multiscale groups, and storing
datasets in `s0`, `s1`, ... `sN`, [conforming with formats 3 and 2](https://github.com/saalfeldlab/paintera/issues/61).
Each scale builds upon the previous one (e.g. `2,2,1 2,2,2` first downscales by 2 in x,y for `s1`, then downscales
*again* by 2 in x,y,z for `s2`).
* `-h` or `--help` shows a less informative version of this explanation.

## Example Usage
```
java -Dspark.master=local[*] -jar target/paintera-conversion-helper-0.0.1-SNAPSHOT.jar \
     --dataset /path/to/root.n5,/path/to/raw,raw  \
     --dataset /path/to/root.n5,/path/to/labels,label \
     --outputN5 /path/to/output-root.n5 --outputgroup converted --blocksize 64,64,64 \
     --scale 2,2,1 3,3,2 2,2,1
```

