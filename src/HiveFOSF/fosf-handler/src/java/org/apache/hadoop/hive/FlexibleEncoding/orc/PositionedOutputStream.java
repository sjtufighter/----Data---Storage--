package org.apache.hadoop.hive.mastiffFlexibleEncoding.orc;

import java.io.IOException;
import java.io.OutputStream;


abstract class PositionedOutputStream extends OutputStream {

	  /**
	   * Record the current position to the recorder.
	   * @param recorder the object that receives the position
	   * @throws IOException
	   */
	  abstract void getPosition(PositionRecorder recorder) throws IOException;

	  /**
	   * Get the memory size currently allocated as buffer associated with this
	   * stream.
	   * @return the number of bytes used by buffers.
	   */
	  abstract long getBufferSize();
	}
