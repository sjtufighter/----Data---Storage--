package FlexibleEncoding.ORC;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * This class is used to hold the contents of streams as they are buffered.
 * The TreeWriters write to the outStream and the codec compresses the
 * data as buffers fill up and stores them in the output list. When the
 * stripe is being written, the whole stream is written to the file.
 */
public class BufferedStream implements OutStream.OutputReceiver {
 public OutStream outStream;
  private final List<ByteBuffer> output = new ArrayList<ByteBuffer>();

  public BufferedStream(String name, int bufferSize,
                 CompressionCodec codec) throws IOException {
    outStream = new OutStream(name, bufferSize, codec, this);
  }

  /**
   * Receive a buffer from the compression codec.
   * @param buffer the buffer to save
   * @throws IOException
   */
  @Override
  public void output(ByteBuffer buffer) {
    output.add(buffer);
  }

  /**
   * Get the number of bytes in buffers that are allocated to this stream.
   * @return number of bytes in buffers
   */
  public long getBufferSize() {
    long result = 0;
    for(ByteBuffer buf: output) {
      result += buf.capacity();
    }
    return outStream.getBufferSize() + result;
  }

  /**
   * Flush the stream to the codec.
   * @throws IOException
   */
  public void flush() throws IOException {
    outStream.flush();
  }

  /**
   * Clear all of the buffers.
   * @throws IOException
   */
  public void clear() throws IOException {
    outStream.clear();
    output.clear();
  }

  /**
   * Check the state of suppress flag in output stream
   * @return value of suppress flag
   */
//  public boolean isSuppressed() {
//    return outStream.isSuppressed();
//  }

  /**
   * Get the number of bytes that will be written to the output. Assumes
   * the stream has already been flushed.
   * @return the number of bytes
   */
  public long getOutputSize() {
    long result = 0;
    for(ByteBuffer buffer: output) {
      result += buffer.remaining();
    }
    return result;
  }

  /**
   * Write the saved compressed buffers to the OutputStream.
   * @param out the stream to write to
   * @throws IOException
   */
  void spillTo(OutputStream out) throws IOException {
    for(ByteBuffer buffer: output) {
      out.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
        buffer.remaining());
    }
  }

  @Override
  public String toString() {
    return outStream.toString();
  }
}
