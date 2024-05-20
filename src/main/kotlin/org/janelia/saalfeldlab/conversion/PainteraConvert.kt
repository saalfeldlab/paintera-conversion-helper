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
		ToScalar::class]
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
//                LOG.info("Cli Return Code: {}", cliReturnCode)

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