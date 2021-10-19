package mr04.custom_writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPairWritable implements WritableComparable<TextPairWritable> {

    private Text first;
    private Text second;

    public TextPairWritable(Text first, Text second) {
        set(first, second);
    }

    public TextPairWritable() {
        set(new Text(), new Text());
    }

    public TextPairWritable(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public String toString() {
        return first + " " + second;
    }

    @Override
    public int compareTo(TextPairWritable tp) {
        int cmp = first.compareTo(tp.first);

        if (cmp != 0) {
            return cmp;
        }

        return second.compareTo(tp.second);
    }

    @Override
    public int hashCode(){
        return Objects.hash(first.hashCode() + second.hashCode());
    }

    @Override
    public boolean equals(Object o)
    {
        if(o instanceof TextPairWritable)
        {
            TextPairWritable tp = (TextPairWritable) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

}