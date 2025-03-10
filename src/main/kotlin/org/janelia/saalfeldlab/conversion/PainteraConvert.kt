package org.janelia.saalfeldlab.conversion

import org.janelia.saalfeldlab.conversion.to.paintera.ToPainteraData
import org.janelia.saalfeldlab.paintera.conversion.to.scalar.ToScalar
import picocli.CommandLine
import java.util.concurrent.Callable
import kotlin.system.exitProcess


@CommandLine.Command(
	name = "paintera-convert",
	subcommands = [
		ToPainteraData.Parameters::class,
		ToScalar::class],
	usageHelpWidth = 120,
	header = ["Conversion tool to and from Paintera datasets."],
	description = [

		"",
		"" +
				"Converts arbitrary 3D label and single- or multi-channel raw datasets in N5, Zarr, or HDF5 containers into a Paintera-friendly format (https://github.com/saalfeldlab/paintera#paintera-data-format).  " +
				"A Paintera-friendly format is a group (referred to as \"paintera group\" in the following) inside an N5 container with a multi-scale representation (mipmap pyramid) in the `data' sub-group. " +
				"The `data' sub-group contains datasets s0 ... sN, where s0 is the highest resolution dataset and sN is the lowest resolution (most downsampled) dataset.  " +
				"Each dataset sX has an attribute `\"downsamplingFactors\":[X, Y, Z]' relative to s0, e.g. `\"downsamplingFactors\":[16.0,16.0,2.0]'.  " +
				"If not specified, `\"downsamplingFactors\":[1.0, 1.0, 1.0]' is assumed (this makes sense only for s0).  " +
				"Unless the `--winner-takes-all-downsampling' option is specified, label data is converted and downsampled with a non-scalar summarizing label type (https://github.com/saalfeldlab/paintera#label-multisets).  " +
				"The highest resolution label dataset can be extracted as scalar UINT64 label type with the `to-scalar' sub-command.  " +
				"The paintera group has a \"painteraData\" attribute to specify the type of the dataset, i.e. `\"painteraData\":{\"type\":\"\$TYPE\"}', " +
				"where TYPE is one of {channel, label, raw}.",
		"",
		"" +
				"Label data paintera groups have additional sub-groups to store unique lists of label ids for each block (`unique-labels') per scale level, " +
				"an index of containing blocks for each label id (`label-to-block-mapping') per scale level, " +
				"and a lookup table for manual agglomeration of fragments (`fragment-segment-assignment').  " +
				"Currently, the lookup table cannot be provided during conversion and will be populated when using Paintera.  " +
				"A mandatory attribute `maxId' in the paintera group keeps track of the largest label id that has been used for a dataset.  " +
				"The `\"labelBlockLookup\"' attribute specifies the type of index stored in `label-to-block-mapping'.",
		"",
		"" +
				"Conversion options can be set at (a) the global level, (b) at the N5/Zarr/HDF5 container level, or (c) at a dataset level.  " +
				"More specific options take precedence over more general option if specified, in particular (b) overrides (a) and (c) overrides (b).  " +
				"Options that override options set at a more general level are prefixed with `--container' and `--dataset' for (b) and (c), respectively.  " +
				"For example, the downsampling factors/scales can be set with the `--scale' option at the global level and overriden with the " +
				"`--container-scale' option at the container level or the `--dataset-scale' option at the dataset level.",
		""]
)
class PainteraConvert : Callable<Unit> {

	@CommandLine.Option(names = ["--help"], help = true, usageHelp = true)
	var helpRequested: Boolean = false

	@CommandLine.Option(names = ["--version"], help = true, versionHelp = true)
	var versionRequested: Boolean = false

	val helpOrVersionRequested: Boolean
		get() = helpRequested || versionRequested

	companion object {

		@JvmStatic
		fun main(args: Array<String>) {
			val arg = PainteraConvert()
			val cli = CommandLine(arg)

			try {
				val cliReturnCode = cli.execute(*args)

				if (cliReturnCode != 0)
					exitProcess(cliReturnCode)

				if (arg.helpOrVersionRequested) {
					if (arg.versionRequested)
						println(Version.VERSION_STRING)
					exitProcess(0)
				}

				val parseResult = cli.parseResult
				if (!parseResult.hasSubcommand()) {
					System.err.println("No command specified!")
					cli.usage(System.err)
					exitProcess(EXIT_CODE_NO_SUBCOMMAND)
				}
			} catch (conversionError: ConversionException) {
				System.err.println(conversionError.message)
				exitProcess(conversionError.exitCode)
			}
		}

		const val EXIT_CODE_SUCCESS = 0
		const val EXIT_CODE_HELP_REQUESTED = 0
		const val EXIT_CODE_INVALID_INPUT = 255
		const val EXIT_CODE_EXECUTION_EXCEPTION = 254
		const val EXIT_CODE_NO_SUBCOMMAND = 253

	}

	override fun call() = Unit

}