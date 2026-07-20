[![Build Status](https://github.com/saalfeldlab/paintera-conversion-helper/actions/workflows/build-main.yml/badge.svg)](https://github.com/saalfeldlab/paintera-conversion-helper/actions/workflows/build-main.yml)
![GitHub Release](https://img.shields.io/github/v/release/saalfeldlab/paintera-conversion-helper)

# Paintera Conversion Helper
Script to assist conversion of n5 datasets to paintera-friendly formats, as specified [here](https://github.com/saalfeldlab/paintera/issues/61).

## Supported Formats

| command       | input containers       | output container |
|:--------------|:-----------------------|:-----------------|
| `to-paintera` | N5, Zarr2, Zarr3, HDF5 | N5               |
| `to-scalar`   | N5, Zarr2, Zarr3, HDF5 | N5, Zarr2, Zarr3 |

- `to-paintera` currently requires `N5` output.
- `to-scalar` Zarr3 output can be sharded, and will write OME-Zarr metadata

## Prebuilt Releases
Prebuilt releases can be downloaded for Ubuntu and MacOS from the [Github Releases](https://github.com/saalfeldlab/paintera-conversion-helper/releases)

---
## <u>Run from Source</u>
### Compile
To compile the conversion helper into a jar, simply run
```
./mvnw package
```

To run locally:
```
./mvnw -q compile exec:java \
  -Dexec.args="to-paintera --container=in.zarr -d labels/s0 \
    --output-container=out.n5 --target-dataset=labels --type=label \
    --scale 2,2,2 2,2,2 --block-size=32,32,32"
```

### Janelia cluster

Clone the repository with submodules:
```
git clone --recursive https://github.com/saalfeldlab/paintera-conversion-helper.git
```
If you have already cloned the repository, run this after cloning to fetch the submodules:
```
git submodule update --init --recursive
```

For submitting a job to the Janelia cluster you can use the following script:
```
./startup-scripts/flintstone-paintera-convert.sh  <flintstone args> -- <paintera-convert args>
```
Importantly:
- the first flintstone arg before `--` is required, and MUST be the number of cluster nodes to use (e.g. `5`)
- the classpath, main class, and additional args are provided to flintstone, so you do not need to specify them

<details>
<summary><b>Cluster Example</b></summary>

```sh
# 5 is passed to flintstone, indicating 5 nodes to use.
./startup-scripts/flintstone-paintera-convert.sh 5 -- to-paintera \
  --scale 2,2,1 2,2,1 2,2,1 2 2 \
  --reverse-array-attributes \
  --output-container=paintera-converted.n5 \
  --container=sample_A_20160501.hdf \
    -d volumes/raw \
      --target-dataset=volumes/raw2 \
      --dataset-scale 3,3,1 3,3,1 2 2 \
      --dataset-resolution 4,4,40.0 \
    -d volumes/labels/neuron_ids
```
</details>

---
## <u>Usage</u>
### To Paintera
This conversion tool currently supports any number of datasets (raw or label) with a
single (global) block size, and will output to a single N5 group in a paintera-compatible
format.

By default, spark will run locally with up to 24 workers. You can specify more with `--spark-master`:
```
paintera-convert to-paintera [...]
```
#### Example

To convert the `raw` and `neuron_ids` datasets of [sample A of the cremi challenge](https://cremi.org/data/) into Paintera format with mipmaps on Linux, assuming that you downloaded the data into `$HOME/Downloads`, run:
```sh
paintera-convert to-paintera \
  --scale 2,2,1 2,2,1 2,2,1 2 2 \
  --reverse-array-attributes \
  --output-container=paintera-converted.n5 \
  --container=sample_A_20160501.hdf \
    -d volumes/raw \
      --target-dataset=volumes/raw2 \
      --dataset-scale 3,3,1 3,3,1 2 2 \
      --dataset-resolution 4,4,40.0 \
    -d volumes/labels/neuron_ids
```
<details>
<summary><b>Usage Help </b></summary>

```
$ paintera-convert to-paintera --help
Usage: paintera-convert to-paintera [--overwrite-existing] [--help] --output-container=OUTPUT_CONTAINER                                                  
                                    [--output-format=OUTPUT_FORMAT] [--spark-master=<sparkMaster>] [[--block-size=X,Y,                                   
                                    Z|U] [--scale=X,Y,Z|U[\sX,Y,Z|U...]...]... [--downsample-block-sizes=X,Y,Z|U[\sX,Y,                                  
                                    Z|U...]...]... [--reverse-array-attributes] [--resolution=X,Y,Z|U] [--offset=X,Y,                                    
                                    Z|U] [-m=N...]... [--label-block-lookup-n5-block-size=N]                                                             
                                    [--winner-takes-all-downsampling]] ([--container=CONTAINER] [] (-d=DATASET                                           
                                    [--target-dataset=TARGET_DATASET] [[--type=TYPE]                                                                     
                                    [--slice-positions=SLICE_POSITIONS]])...)...                                                                         
      --block-size=X,Y,Z|U   
                            Use --container-block-size and --dataset-block-size for container and dataset specific                                      
                               block sizes, respectively.                                                                                                
                            Can be set globally, per container, or per dataset                                                                          
      --scale=X,Y,Z|U[\sX,Y,Z|U...]...                                      
                             Relative downsampling factors for each level in the format x,y,z, where x,y,z are                                           
                               integers. Single integers u are interpreted as u,u,u.                                                                     
                             Use --container-scale and --dataset-scale for container and dataset specific scales,                                        
                               respectively.                                
                             Can be set globally, per container, or per dataset                                                                          
      --downsample-block-sizes=X,Y,Z|U[\sX,Y,Z|U...]...                                                                                                  
                             Use --container-downsample-block-sizes and --dataset-downsample-block-sizes for container                                   
                               and dataset specific block sizes, respectively.                                                                           
                             Can be set globally, per container, or per dataset                                                                          
      --reverse-array-attributes                                            
                             Reverse array attributes like resolution and offset, i.e. [x, y, z] -> [z, y, x].                                           
                             Use --container-reverse-array-attributes and --dataset-reverse-array-attributes for                                         
                               container and dataset specific setting, respectively.                                                                     
                             Can be set globally, per container, or per dataset                                                                          
      --resolution=X,Y,Z|U   Specify resolution (overrides attributes of input datasets, if any).                                                        
                             Use --container-resolution and --dataset-resolution for container and dataset specific                                      
                               resolution, respectively.                                                                                                 
                             Can be set globally, per container, or per dataset                                                                          
      --offset=X,Y,Z|U       Specify offset (overrides attributes of input datasets, if any).                                                            
                             Use --container-offset and --dataset-offset for container and dataset specific resolution,                                  
                               respectively.                                
                             Can be set globally, per container, or per dataset                                                                          
  -m, --max-num-entries=N... Limit number of entries for non-scalar label types by N. If N is negative, do not limit                                     
                               number of entries.  If fewer values than the number of down-sampling layers are                                           
                               provided, the missing values are copied from the last available entry.  If none are                                       
                               provided, default to -1 for all levels.                                                                                   
                             Use --container-max-num-entries and --dataset-max-num-entries for container and dataset                                     
                               specific settings, respectively.                                                                                          
                             Can be set globally, per container, or per dataset                                                                          
      --label-block-lookup-n5-block-size=N                                  
                             Set the block size for the N5 container for the label-block-lookup.                                                         
                             Use --container-label-block-lookup-n5-block-size and                                                                        
                               --dataset-label-block-lookup-n5-block-size for container and dataset specific settings,                                   
                               respectively.                                
                             Can be set globally, per container, or per dataset                                                                          
      --winner-takes-all-downsampling                                       
                             Use scalar label type with winner-takes-all downsampling.                                                                   
                             Use --container-winner-takes-all-downsampling and --dataset-winner-takes-all-downsampling                                   
                               for container and dataset specific settings, respectively.                                                                
                             Can be set globally, per container, or per dataset                                                                          
      --overwrite-existing                                                  
      --output-container=OUTPUT_CONTAINER                                   

      --output-format=OUTPUT_FORMAT                                         

      --spark-master=<sparkMaster>                                          
                             Spark master URL. Default will run locally with up to 24 workers (e.g. loca[24] ).                                          
      --container=CONTAINER                                                 
  -d, --dataset=DATASET               
      --target-dataset=TARGET_DATASET                                       

      --type=TYPE                     
      --slice-positions=SLICE_POSITIONS                                     
                             Reduce an nD input to a 3D XYZ Paintera source. One argument per input axis in source                                       
                               dimension order: `x'/`y'/`z' indicate a spatial index, an integer determines where to                                     
                               slice at that axis (e.g. `z,y,x,0,10').                                                                                   
      --help                          

```
</details>

___

### To Scalar

`paintera-convert` can also convert from paintera label sources to scalar label datasets:
```
paintera-convert to-scalar [...]
```
`to-scalar` will extract the highest resolution scale level of a Paintera dataset as a multiscale `uint64` dataset. 
This is useful for using Paintera painted labels (and assignments) in downstream processing, e.g. classifier training. 
Optionally, the `fragment-segment-assignment` can be considered and additional assignments can be added. 
See `to-scalar --help` for more details.

<details>
<summary><b>Usage Help</b></summary>

```
Usage: paintera-convert to-scalar [--consider-fragment-segment-assignment]
                                  [--help] -i=<inputContainer>
                                  -I=<inputDataset> -o=<_outputContainer>
                                  [-O=<outputDataset>] [--offset=X,Y,Z|U]
                                  [--output-format=OUTPUT_FORMAT]
                                  [--resolution=X,Y,Z|U]
                                  [--spark-master=<sparkMaster>]
                                  [--additional-assignment=from=to[,
                                  from=to...]]... [--block-size=<blockSize>[,
                                  <blockSize>...]]...
                                  [--chunks-per-shard=<chunksPerShard>[,
                                  <chunksPerShard>...]]... --xyz-unit=<xyzUnit>
                                  [,<xyzUnit>...] [--xyz-unit=<xyzUnit>[,
                                  <xyzUnit>...]]... [--downsample-block-sizes=X,
                                  Y,Z|U[\sX,Y,Z|U...]...]... [--scale=X,Y,Z|U
                                  [\sX,Y,Z|U...]...]...
Convert non-scalar label data to UINT64 scalar dataset.  The input can be a
single-scale dataset, a multi-scale group, or a Paintera dataset.
      --additional-assignment=from=to[,from=to...]
                             Add additional lookup-values in the format
                               `from=to'. Warning: Consistency with
                               fragment-segment-assignment is not enforced.
      --block-size=<blockSize>[,<blockSize>...]
                             Block size for output dataset. Will default to
                               block size of input dataset if not specified.
      --chunks-per-shard=<chunksPerShard>[,<chunksPerShard>...]
                             Number of chunks per shard, per axis (one value or
                               three). Only valid for OUTPUT_FORMAT=ZARR3
      --consider-fragment-segment-assignment
                             Consider fragment-segment-assignment inside
                               Paintera dataset. Will be ignored if not a
                               Paintera dataset
      --downsample-block-sizes=X,Y,Z|U[\sX,Y,Z|U...]...
                             Output block size per downsampled level; defaults
                               to --block-size for every level.
  -i, --input-container=<inputContainer>

  -I, --input-dataset=<inputDataset>
                             Can be a Paintera dataset, multi-scale N5 group,
                               or regular dataset. Highest resolution dataset
                               will be used for Paintera dataset (data/s0) and
                               multi-scale group (s0).
  -o, --output-container=<_outputContainer>

  -O, --output-dataset=<outputDataset>
                             defaults to input dataset
      --offset=X,Y,Z|U       Physical offset x,y,z (a single value u means u,u,
                               u). Overrides the input's offset attribute.
      --output-format=OUTPUT_FORMAT

      --resolution=X,Y,Z|U   Physical resolution x,y,z (a single value u means
                               u,u,u). Overrides the input's resolution
                               attribute; needed for Paintera inputs, which
                               store it on the data group rather than s0.
      --scale=X,Y,Z|U[\sX,Y,Z|U...]...
                             Relative downsampling factors for each level in
                               the format x,y,z, where x,y,z are integers.
                               Single integers u are interpreted as u,u,u.
      --spark-master=<sparkMaster>
                             Spark master URL. Default will run locally with up
                               to 24 workers (e.g. local[24] ).
      --xyz-unit=<xyzUnit>[,<xyzUnit>...]
                             Unit for the x, y, z (1 value, or 1 value per
                               axis).
      --help
```
</details>