package org.janelia.saalfeldlab.conversion

import net.imglib2.RandomAccessibleInterval
import net.imglib2.algorithm.util.Singleton
import net.imglib2.algorithm.util.Singleton.ThrowingSupplier
import net.imglib2.cache.img.DiskCachedCellImg
import net.imglib2.cache.img.DiskCachedCellImgFactory
import net.imglib2.cache.img.DiskCachedCellImgOptions
import net.imglib2.type.NativeType
import net.imglib2.type.numeric.integer.UnsignedLongType
import net.imglib2.util.Intervals
import net.imglib2.view.Views
import org.apache.spark.api.java.function.Function0
import org.janelia.saalfeldlab.label.spark.convert.ConvertToLabelMultisetType
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import java.io.Serializable

/**
 * Parsed `--slice-positions`: for an nD source with 3+ axes, which input axis maps to each output spatial axis and
 * where the extra axes are sliced.
 * `x`/`y`/`z` assign the spatial role, integers slice on that axis.
 *
 * @property spatialInputDims input dim index mapping to output x, y, z (length 3)
 * @property slicedAt input dim index -> the slice position, for every non-spatial axis
 */
data class SliceSpec(val spatialInputDims: IntArray, val slicedAt: Map<Int, Long>) : Serializable {
	val outputDimensions: LongArray
		get() = LongArray(3) { fullDim[spatialInputDims[it]] }
	internal lateinit var fullDim: LongArray

	override fun equals(other: Any?) = other is SliceSpec && spatialInputDims.contentEquals(other.spatialInputDims) && slicedAt == other.slicedAt
	override fun hashCode() = spatialInputDims.contentHashCode() * 31 + slicedAt.hashCode()
}

class InvalidSlicePositions(message: String) : IllegalArgumentException(message)

/**
 * Parse `spec` (e.g. `z,y,x,0,10`) against a dataset of the given dimensions. One per axis; exactly one each of
 * x, y, z; every other entry must be a valid slice index.
 */
fun parseSlicePositions(spec: String, dimensions: LongArray): SliceSpec {
	val tokens = spec.split(",").map { it.trim() }
	if (tokens.size != dimensions.size)
		throw InvalidSlicePositions("--slice-positions has ${tokens.size} tokens but the dataset has ${dimensions.size} dimensions")

	val spatial = HashMap<Char, Int>()
	val slicedAt = HashMap<Int, Long>()
	tokens.forEachIndexed { dim, token ->
		when (val lower = token.lowercase()) {
			"x", "y", "z" -> {
				val axis = lower[0]
				if (axis in spatial)
					throw InvalidSlicePositions("spatial axis `$axis' assigned more than once in --slice-positions")
				spatial[axis] = dim
			}
			else -> {
				val index = token.toLongOrNull()
					?: throw InvalidSlicePositions("token `$token' at axis $dim is neither x/y/z nor an integer slice index")
				if (index < 0 || index >= dimensions[dim])
					throw InvalidSlicePositions("slice $index at axis $dim is out of bounds [0, ${dimensions[dim]})")
				slicedAt[dim] = index
			}
		}
	}
	if (spatial.keys != setOf('x', 'y', 'z'))
		throw InvalidSlicePositions("--slice-positions must assign exactly one each of x, y, z; got ${spatial.keys.sorted()}")

	return SliceSpec(intArrayOf(spatial.getValue('x'), spatial.getValue('y'), spatial.getValue('z')), slicedAt)
		.also { it.fullDim = dimensions.clone() }
}

/**
 * Reduce an nD `source` to a 3D XYZ view per `spec`.
 */
fun <T> sliceTo3D(source: RandomAccessibleInterval<T>, spec: SliceSpec): RandomAccessibleInterval<T> {
	var view = source
	for (dim in spec.slicedAt.keys.sortedDescending())
		view = Views.hyperSlice(view, dim, spec.slicedAt.getValue(dim))

	/* view is now 3D; every non-sliced axis is spatial, so the remaining axes are the 3 spatial input dimensions in
	 * original order. map each spatial input dim to its index in the reduced view, then permute into (x, y, z). */
	val newIndexOf = spec.spatialInputDims.sorted().withIndex().associate { (newIndex, origDim) -> origDim to newIndex }
	val order = IntArray(3) { output -> newIndexOf.getValue(spec.spatialInputDims[output]) }
	return permuteAxes(view, order)
}

/** Reorder axes so that [result] has axis order [order]. */
private fun <T> permuteAxes(view: RandomAccessibleInterval<T>, order: IntArray): RandomAccessibleInterval<T> {
	var result = view
	val axisAt = IntArray(order.size) { it }     // axisAt[position] = source axis currently at that position
	val positionOf = IntArray(order.size) { it } // positionOf[sourceAxis] = its current position
	for (output in order.indices) {
		val want = order[output]
		val have = axisAt[output]
		if (have == want) continue
		val wantPosition = positionOf[want]
		result = Views.permute(result, output, wantPosition)
		axisAt[output] = want; axisAt[wantPosition] = have
		positionOf[want] = output; positionOf[have] = wantPosition
	}
	return result
}

/**
 * Create [sliceTo3D] as a lazy, disk-cached 3D image. Cell size defaults to `blockSize`.
 */
fun <T : NativeType<T>> diskCachedSlice(
	source: RandomAccessibleInterval<T>,
	spec: SliceSpec,
	type: T,
	blockSize: IntArray
): DiskCachedCellImg<T, *> {
	val sliced = sliceTo3D(source, spec)
	val options = DiskCachedCellImgOptions.options().cellDimensions(*blockSize)
	return DiskCachedCellImgFactory(type, options).create(Intervals.dimensionsAsLongArray(sliced)) { cell ->
		val src = Views.flatIterable(Views.interval(sliced, cell)).cursor()
		val dst = Views.flatIterable(cell).cursor()
		while (dst.hasNext()) dst.next().set(src.next())
	}
}

/**
 * Serializable, per-executor-cached supplier of the sliced 3D input for [ConvertToLabelMultisetType].
 */
fun slicedInputImgSupplier(
	inputContainer: String,
	inputDataset: String,
	spec: SliceSpec,
	blockSize: IntArray
): Function0<RandomAccessibleInterval<UnsignedLongType>> = Function0 {
    val cacheKey = "sliced-input::$inputContainer::$inputDataset::${spec.spatialInputDims.toList()}::${spec.slicedAt}"
    @Suppress("UNCHECKED_CAST")
    Singleton.get(cacheKey, ThrowingSupplier {
        openSlicedDiskCached<UnsignedLongType>(inputContainer, inputDataset, spec, blockSize)
    }) as RandomAccessibleInterval<UnsignedLongType>
}

private fun <T> openSlicedDiskCached(
	inputContainer: String,
	inputDataset: String,
	spec: SliceSpec,
	blockSize: IntArray
): DiskCachedCellImg<T, *> where T : NativeType<T> {
	val reader = createReader(inputContainer)
	val type: T = N5Utils.type(reader.getDatasetAttributes(inputDataset).dataType)
	val nd: RandomAccessibleInterval<T> = N5Utils.open(reader, inputDataset)
	return diskCachedSlice(nd, spec, type, blockSize)
}
