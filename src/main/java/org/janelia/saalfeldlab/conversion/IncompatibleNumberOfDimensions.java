package org.janelia.saalfeldlab.conversion;

import java.util.Arrays;

public class IncompatibleNumberOfDimensions extends ConverterException {

    private final int actualNumberOfDimension;

    private final int[] acceptableNumbersOfDimension;

    public IncompatibleNumberOfDimensions(
            final int actualNumberOfDimension,
            final int... acceptableNumbersOfDimension) {
        super(makeMessage(actualNumberOfDimension, acceptableNumbersOfDimension));
        this.actualNumberOfDimension = actualNumberOfDimension;
        this.acceptableNumbersOfDimension = acceptableNumbersOfDimension.clone();
    }

    private static String makeMessage(final int actualNumberOfDimension, final int[] acceptableNumbersOfDimension) {
        return String.format(
                "Number of dimensions %d is not supported. Supported numbers of dimensions: %s",
                actualNumberOfDimension,
                Arrays.toString(acceptableNumbersOfDimension)
        );
    }
}
