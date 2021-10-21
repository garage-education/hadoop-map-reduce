# Lab Goal

In this lab, we will Create `Custom Key Writable` in Hadoop MapReduce.

# Problem Background

## What is Writable?

Writable is

- A serializable object which implements a simple, efficient, serialization protocol, based on DataInput and DataOutput.
  Any key or value type in the Hadoop Map-Reduce framework implements this interface.
  Ref: [Hadoop Docs](https://hadoop.apache.org/docs/r3.0.1/api/org/apache/hadoop/io/Writable.html)

In some use cases we need to pass custom objects and these custom objects must implements the Writable interface.

## Why does Hadoop use Writable(s)?

As we already know, data needs to be transmitted between different nodes in a distributed computing environment. This
requires serialization and deserialization of data to convert the data that is in structured format to byte stream and
vice-versa. Hadoop therefore uses simple and efficient serialization protocol to serialize data between map and reduce
phase and these are called Writable(s). Some of the examples of writables as already mentioned before are IntWritable,
LongWritable, BooleanWritable and FloatWritable.

Ref: https://stackoverflow.com/a/32518079

# Steps to create a custom writable type

- Implement `Writable` Interface
  Example
```java 
public class StringPairWritable implements WritableComparable<StringPairWritable> 
```
- Add `Default Constructor`

  Example:
```java
 /**
 * Empty constructor - required for serialization.
 */

public StringPairWritable() {
}

/**
 * Constructor with two String objects provided as input.
 */
public StringPairWritable(String left, String right) {
    this.left = left;
    this.right = right;
}
```

For more information about Why does Hadoop need empty Constructor? 

- https://www.javatpoint.com/java-reflection
- https://www.javatpoint.com/new-instance()-method
- https://stackoverflow.com/a/18099352/2516356
- https://stackoverflow.com/a/11447050/2516356
- https://qr.ae/pGVLoh

- Override `write` method
```java 
@Override
public void write(DataOutput out) throws IOException {
        out.writeUTF(this.left);
        out.writeUTF(this.right);
    }
```
- Override `readFields` method
```java 
@Override
public void readFields(DataInput in) throws IOException {
        this.left = in.readUTF();
        this.right = in.readUTF();
    }
```

- Override `hashCode` method
```java
@Override
public int hashCode(){
    return Objects.hash(left, right);
}
```

- Override `equals` method
```java
@Override
public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StringPairWritable that = (StringPairWritable) o;
        return left.equals(that.left) && right.equals(that.right);
        }
```

## Applications

- Any application with custom objects as key.
  * GeoLocation applications lat,long
  * Custom address objects city, street, postcode.
  
## Homework Labs

