package ORC;

/**
adapted from ORC
@author wangmeng
 */


import java.io.IOException;

/**
 * A streamFactory that writes a sequence of integers. A control byte is written before
 * each run with positive values 0 to 127 meaning 3 to 130 repetitions, each
 * repetition is offset by a delta. If the control byte is -1 to -128, 1 to 128
 * literal vint values follow.
 */
class RunLengthIntegerWriter {
  static final int MIN_REPEAT_SIZE = 3;
  static final int MAX_DELTA = 127;
  static final int MIN_DELTA = -128;
  static final int MAX_LITERAL_SIZE = 128;
  private static final int MAX_REPEAT_SIZE = 127 + MIN_REPEAT_SIZE;
  private final PositionedOutputStream output;
  private final boolean signed;
  private final long[] literals = new long[MAX_LITERAL_SIZE];
  private int numLiterals = 0;
  private long delta = 0;
  private boolean repeat = false;
  private int tailRunLength = 0;

  RunLengthIntegerWriter(PositionedOutputStream output,
                         boolean signed) {
    this.output = output;
    this.signed = signed;
  }

  private void writeValues() throws IOException {
    if (numLiterals != 0) {
      if (repeat) {
        output.write(numLiterals - MIN_REPEAT_SIZE);
        output.write((byte) delta);
        if (signed) {
          SerializationUtils.writeVslong(output, literals[0]);
        } else {
          SerializationUtils.writeVulong(output, literals[0]);
        }
      } else {
        output.write(-numLiterals);
        for(int i=0; i < numLiterals; ++i) {
          if (signed) {
            SerializationUtils.writeVslong(output, literals[i]);
          } else {
            SerializationUtils.writeVulong(output, literals[i]);
          }
        }
      }
      repeat = false;
      numLiterals = 0;
      tailRunLength = 0;
    }
  }

  void flush() throws IOException {
    writeValues();
    output.flush();
  }

  void write(long value) throws IOException {
    if (numLiterals == 0) {
      literals[numLiterals++] = value;
      tailRunLength = 1;
    } else if (repeat) {
      if (value == literals[0] + delta * numLiterals) {
        numLiterals += 1;
        if (numLiterals == MAX_REPEAT_SIZE) {
          writeValues();
        }
      } else {
        writeValues();
        literals[numLiterals++] = value;
        tailRunLength = 1;
      }
    } else {
      if (tailRunLength == 1) {
        delta = value - literals[numLiterals - 1];
        if (delta < MIN_DELTA || delta > MAX_DELTA) {
          tailRunLength = 1;
        } else {
          tailRunLength = 2;
        }
      } else if (value == literals[numLiterals - 1] + delta) {
        tailRunLength += 1;
      } else {
        delta = value - literals[numLiterals - 1];
        if (delta < MIN_DELTA || delta > MAX_DELTA) {
          tailRunLength = 1;
        } else {
          tailRunLength = 2;
        }
      }
      if (tailRunLength == MIN_REPEAT_SIZE) {
        if (numLiterals + 1 == MIN_REPEAT_SIZE) {
          repeat = true;
          numLiterals += 1;
        } else {
          numLiterals -= MIN_REPEAT_SIZE - 1;
          long base = literals[numLiterals];
          writeValues();
          literals[0] = base;
          repeat = true;
          numLiterals = MIN_REPEAT_SIZE;
        }
      } else {
        literals[numLiterals++] = value;
        if (numLiterals == MAX_LITERAL_SIZE) {
          writeValues();
        }
      }
    }
  }

  void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(numLiterals);
  }
}
