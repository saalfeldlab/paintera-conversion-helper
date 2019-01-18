package org.janelia.saalfeldlab.conversion;

public class IncompatibleChannelAxis extends ConverterException {

    private final int numDimensions;

    private final int channelAxis;


    public IncompatibleChannelAxis(
            final int numDimensions,
            final int channelAxis) {
        super(String.format("Invalid channel axis: %d. Constraints on channel axis: 0 <= channel axis < %d", channelAxis, numDimensions));
        this.numDimensions = numDimensions;
        this.channelAxis = channelAxis;
    }
}
