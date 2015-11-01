import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class MSWDRecordReader implements RecordReader<LongWritable, Text> {

    private final static Text EOL = new Text("\n");
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private FSDataInputStream fileIn;
    int maxLineLength;
    private Text bufLookAhead = new Text("");
    private int offsetLookAhead; // bytes read into bufLookAhead, not equal to length of data stored there.

    private static final Log LOG = LogFactory.getLog(
            MSWDRecordReader.class);

    public MSWDRecordReader(Configuration job, FileSplit split,
                            byte[] lineDelimiter) throws IOException {
        this.maxLineLength = job.getInt(org.apache.hadoop.mapreduce.lib.input.
                LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        fileIn.seek(start);
        in = new LineReader(fileIn, job, lineDelimiter);
        // If this is not the first split, we always throw away first partial record
        // because we always (except the last split) read one extra record in
        // next() method.
        if (start != 0) {
            start += readNext(new Text(), maxLineLength, maxBytesToConsume(start)); // ??? Why maxLineLength == 0 here?
        }
        this.pos = start;
    }

    private int maxBytesToConsume(long pos) {
        return (int) Math.min(Integer.MAX_VALUE, end - pos);
    }

    // Read next multi-line record.
    private int readNext(Text text,
                         int maxLineLength,
                         int maxBytesToConsume)
            throws IOException {

        int offset = 0;
        text.clear();
        Text tmp = new Text();

        // fetch content of the look ahead buf.
        text.append(bufLookAhead.getBytes(), 0, bufLookAhead.getLength());
        offset = offsetLookAhead; // offset counts also EOL, which is not saved by readLine in Text buf.
        bufLookAhead.clear();

        for (int i = 0; i < maxBytesToConsume; i++) {

            int offsetTmp = in.readLine(
                    tmp,
                    maxLineLength,
                    maxBytesToConsume);

            // End of File
            if (offsetTmp == 0) {
                break;
            }

            String s = tmp.toString();
            if (isOneLineRecord(s)) { // is single line record, stop reading.
                text.append(tmp.getBytes(), 0, tmp.getLength());
                offset += offsetTmp;
                break;
            } else if (i > 0 && isNewRecord(s)) { // not first line in this read and is a new record line.
                bufLookAhead.append(tmp.getBytes(), 0, tmp.getLength()); // save next record line for next read.
                offsetLookAhead = offsetTmp;
                // Begin of next record, don't count this line in offset, but keep it in bufLookAhead for later use.
                break;
            } else { // not start of record, continue reading. V lines or incomplete lines at start of split.
                // Append line to record
                if (offset >0) text.append(EOL.getBytes(), 0, EOL.getLength()); // prepend EOF if text buf not empty.
                text.append(tmp.getBytes(), 0, tmp.getLength());
                offset += offsetTmp;
            }
        }
        return offset;
    }

    /* Check whether it's a single line record.
     * Records start with: I,T,N,A, are single line records.
     */
    private boolean isOneLineRecord(String s) {
        return
            s.startsWith("I,") ||
            s.startsWith("T,") ||
            s.startsWith("N,") ||
            s.startsWith("A,");
    }

    /* Check whether we see the begin of next record.
     * Records start with: I,T,N,A,C.  Lines starting with V should be parsed as in the same record as previous C line.
     */
    private boolean isNewRecord(String s) {
        return
            s.startsWith("I,") ||
            s.startsWith("T,") ||
            s.startsWith("N,") ||
            s.startsWith("A,") ||
            s.startsWith("C,");
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public Text createValue() {
        return new Text();
    }

    /** Read a record. */
    public synchronized boolean next(LongWritable key, Text value)
            throws IOException {

        // We always read one extra record, which lies outside the upper
        // split limit i.e. (end - 1)
        while (pos <= end) {
            key.set(pos);

            int newSize = readNext(value, maxLineLength,
                    Math.max(maxBytesToConsume(pos), maxLineLength));
            if (newSize == 0) {
                return false;
            }
            pos += newSize;
            if (newSize < maxLineLength) {
                return true;
            }

            // line too long. try again
            LOG.info("Skipped record of size " + newSize + " at pos " + (pos - newSize));
        }

        return false;
    }

    public synchronized long getPos() throws IOException {
        return pos;
    }

    public float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}
