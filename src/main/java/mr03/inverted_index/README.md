# Lab Goal
In this lab, we will write a Java MapReduce job that produces an inverted index.

# Problem Background

## What is an inverted index?

In computer science, an inverted index (also referred to as a postings file or inverted file) 
is a database index storing a mapping from content, such as words or numbers, to its locations in a table, or in a document or a set of documents (named in contrast to a forward index, which maps from documents to content).
Ref: [Wikipedia](https://en.wikipedia.org/wiki/Inverted_index)

## What is the purpose of an inverted index?

The purpose of an inverted index is to allow fast full-text searches, at a cost of increased processing when a document is added to the database. 
The inverted file may be the database file itself, rather than its index. It is the most popular data structure used in document retrieval systems, used on a large scale for example in search engines. Additionally, several significant general-purpose mainframe-based database management systems have used inverted list architectures, including ADABAS, DATACOM/DB, and Model 204.

Ref: [Wikipedia](https://en.wikipedia.org/wiki/Inverted_index)

## Applications

The inverted index data structure is a central component of a typical search engine indexing algorithm. A goal of a search engine implementation is to optimize the speed of the query: find the documents where word X occurs. Once a forward index is developed, which stores lists of words per document, it is next inverted to develop an inverted index. Querying the forward index would require sequential iteration through each document and to each word to verify a matching document. The time, memory, and processing resources to perform such a query are not always technically realistic. Instead of listing the words per document in the forward index, the inverted index data structure is developed which lists the documents per word.

With the inverted index created, the query can now be resolved by jumping to the word ID (via random access) in the inverted index.
Ref: [Wikipedia](https://en.wikipedia.org/wiki/Inverted_index)

# Example

In our previous labs we calculate the word frequency for all documents, but in this example we need to calculate the word frequency per document.

This example is adapted from this [blog post](https://programmer.group/inverted-index-for-mapreduce-programming-development.html). To achieve this, we need to follow the following steps:

- We map the input for each file to be LineNumber, LineText to (word@file_name , word_frequency),

Ex:
Input

## File 1: file1.txt


| LineNumber      | Lines |
| ----------- | ----------- |
| 1      | The best preparation for tomorrow is doing your best today.        |
| 2   | Today a reader, tomorrow a leader.        |
| 3   | What you do today can improve all your tomorrows.        |

## File 2: file2.txt

| LineNumber      | Lines |
| ----------- | ----------- |
| 1      | Someone is sitting in the shade today because someone planted a tree a long time ago.        |


Output

```
<The@file1, 1>
<Today@file1, 1>
<Today@file1, 1>
<Today@file1, 1>
<The@file2, 1>
<Today@file2, 1>
```

- We use the combiner stage to combine the word frequency per document. This step will get the total word frequency per document.
  Ex:
Input:
```
<The@file1, (1)>
<Today@file1, (1,1,1)>
<The@file2, (1)>
<Today@file2, (1)>
```
Output:
```
<The@file1, 1>
<Today@file1, 3>
<The@file2, 1>
<Today@file2, 1>
```

- Last step is the reduce.

Input:
```
<The@file1, 1>
<Today@file1, 3>
<The@file2, 1>
<Today@file2, 1>
```

  Output:
```
<The , (file1:1,file2:1)>
<Today, (file1:3,file2:1)>
```


| word      | Inverted Index |
| ----------- | ----------- |
| The      | file1@1,file2@1|
| today      | file1@3,file2@1|

