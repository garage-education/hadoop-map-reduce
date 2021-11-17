package mr06.secondary_sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The DateTemperaturePair class enable us to represent a
 * composite type of (yearMonth, day, temperature). To persist
 * a composite type (actually any data type) in Hadoop, it has
 * to implement the org.apache.hadoop.io.Writable interface.
 *
 * To compare composite types in Hadoop, it has to implement
 * the org.apache.hadoop.io.WritableComparable interface.
 *
 * @author Mahmoud Parsian
 *
 */
public class DateTemperaturePairWithoutTemp
        implements Writable, WritableComparable<DateTemperaturePairWithoutTemp> {

    private final Text yearMonth = new Text();

    //private final IntWritable temperature = new IntWritable();


    public DateTemperaturePairWithoutTemp() {
    }

    public DateTemperaturePairWithoutTemp(String yearMonth) {
        this.yearMonth.set(yearMonth);

        //this.temperature.set(temperature);
    }

    public static DateTemperaturePairWithoutTemp read(DataInput in) throws IOException {
        DateTemperaturePairWithoutTemp pair = new DateTemperaturePairWithoutTemp();
        pair.readFields(in);
        return pair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        yearMonth.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        yearMonth.readFields(in);

    }

    @Override
    public int compareTo(DateTemperaturePairWithoutTemp pair) {
        int compareValue = this.yearMonth.compareTo(pair.getYearMonth());

        return -1*compareValue;     // to sort descending
    }


    public Text getYearMonth() {
        return yearMonth;
    }


    public void setYearMonth(String yearMonthAsString) {
        yearMonth.set(yearMonthAsString);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DateTemperaturePairWithoutTemp that = (DateTemperaturePairWithoutTemp) o;

        if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = yearMonth != null ? yearMonth.hashCode() : 0;
        result = 31 * result;// + (temperature != null ? temperature.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DateTemperaturePair{yearMonth=");
        builder.append(yearMonth);
        builder.append("}");
        return builder.toString();
    }
}