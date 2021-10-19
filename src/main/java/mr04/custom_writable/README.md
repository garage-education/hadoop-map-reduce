# Lab Goal

In this lab, we will implement `Hadoop Create Custom Key Writable`

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
public class TextPairWritable implements WritableComparable<StringPairWritable> {
```
- Add `Default Constructor`

  Example:
```java
public TextPairWritable(Text first, Text second) {
    set(first, second);
}

public TextPairWritable() {
    set(new Text(), new Text());
}

public TextPairWritable(String first, String second) {
    set(new Text(first), new Text(second));
}
```

- Override `write` method
```java 
@Override
public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
}
```
- Override `readFields` method
```java 
@Override
public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
}
```

- Override `hashCode` method
```java
@Override
public int hashCode(){
    return Objects.hash(first.hashCode() + second.hashCode());
}
```

- Override `equals` method
```java
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
```

## Applications

- Any application with custom objects as key.
  * GeoLocation applications lat,long
  * Custom address objects city, street, postcode.