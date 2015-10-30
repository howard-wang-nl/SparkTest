import com.google.common.base.Charsets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class MSWDInputFormat
    extends TextInputFormat{

        @Override
        public RecordReader<LongWritable, Text> getRecordReader(
                InputSplit genericSplit, JobConf job,
                Reporter reporter)
                throws IOException {

            reporter.setStatus(genericSplit.toString());
            String delimiter = job.get("textinputformat.record.delimiter");
            byte[] recordDelimiterBytes = null;
            if (null != delimiter) {
                recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
            }
            return new MSWDRecordReader(job, (FileSplit) genericSplit,
                    recordDelimiterBytes);
        }
}


