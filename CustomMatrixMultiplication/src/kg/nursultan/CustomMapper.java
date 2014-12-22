package kg.nursultan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CustomMapper extends Mapper<Text, MatrixCellWritable, Text, Text> {
    public void map(Text key, MatrixCellWritable value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int m = Integer.parseInt(conf.get("m"));
        int p = Integer.parseInt(conf.get("p"));
        //String line = value.toString();
        //String[] indicesAndValue = line.split(",");
        Text outputKey = new Text();
        Text outputValue = new Text();
        if (key.toString().equals("A")) {
            for (int k = 0; k < p; k++) {
                outputKey.set(value.rowIndex + "," + k);
                outputValue.set("A," + value.columnIndex + "," + value.value);
                context.write(outputKey, outputValue);
            }
        } else {
            for (int i = 0; i < m; i++) {
                outputKey.set(i + "," + value.columnIndex);
                outputValue.set("B," + value.rowIndex + "," + value.value);
                context.write(outputKey, outputValue);
            }
        }
    }
}