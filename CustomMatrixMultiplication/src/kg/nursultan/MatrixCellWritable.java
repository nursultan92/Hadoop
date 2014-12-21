package kg.nursultan;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatrixCellWritable implements WritableComparable {

    public int columnIndex;
    public int rowIndex;
    public float value;

    public MatrixCellWritable(int rowIndex, int columnIndex, float value) {
        this.columnIndex = columnIndex;
        this.rowIndex = rowIndex;
        this.value = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(rowIndex);
        dataOutput.writeInt(columnIndex);
        dataOutput.writeFloat(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rowIndex = dataInput.readInt();
        columnIndex = dataInput.readInt();
        value = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return Integer.toString(rowIndex) + ", " + Integer.toString(columnIndex);
    }

    @Override
    public int compareTo(Object o) {
        return 1;
    }
}
