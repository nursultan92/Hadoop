package kg.nursultan;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Created by nursultan on 20.12.2014.
 */
public class MatrixRecordReader extends RecordReader {

    private long pos;
    private long start;
    private long end;
    private LineReader lineReader;
    private int maxLineLength;
    private Text key = new Text();
    private Text value = new Text();
    private MatrixCellWritable writable = null;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) inputSplit;

        Configuration configuration = taskAttemptContext.getConfiguration();
        maxLineLength = configuration.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();

        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(configuration);
        FSDataInputStream fileIn = fs.open(split.getPath());

        boolean skipFirstLine = false;
        if (start != 0) {
            skipFirstLine = true;
            --start;
            fileIn.seek(start);
        }
        lineReader = new LineReader(fileIn, configuration);
        if (skipFirstLine) {
            Text dummy = new Text();
            start += lineReader.readLine(dummy, 0,
                    (int) Math.min(
                            (long) Integer.MAX_VALUE,
                            end - start));
        }
        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        int newSize = 0;

        while (pos < end) {

            newSize = lineReader.readLine(value, maxLineLength,
                    Math.max((int) Math.min(
                                    Integer.MAX_VALUE, end - pos),
                            maxLineLength));

            if (newSize == 0) {
                break;
            }

            pos += newSize;


            if (newSize < maxLineLength) {

                createKeyValue(value);

                break;
            }

        }


        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

    private void createKeyValue(Text line) {
        String[] fields = line.toString().split(",");
        key.set(fields[0]);
        writable = new MatrixCellWritable(Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Float.parseFloat(fields[3]));
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return writable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        if (lineReader != null) {
            lineReader.close();
        }
    }
}
