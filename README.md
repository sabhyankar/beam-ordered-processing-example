# Beam PTransform to process elements in order.

[ProcessInOrder](src/main/java/com/google/cloud/pso/transforms/ProcessInOrder.java) is a [PTransform](https://beam.apache.org/releases/javadoc/2.14.0/org/apache/beam/sdk/transforms/PTransform.html) that
will take a PCollection<KV<T, KV<K,V>>> and returns a PCollection<KV<T, Iterable<KV<K,V>>>>.

Where:

* T - Primary key to group the elements.
* K - Secondary key that will used for sorting the values V.
* V - Values to be sorted.

This transform will first group the [PCollection](https://beam.apache.org/releases/javadoc/2.14.0/org/apache/beam/sdk/values/PCollection.html) by T and for each distinct value of T,
it will sort the KV<K, V> using K as the sorting key. Sorting is done using lexicographic comparison of the byte representations of the secondary keys.

This sample pipeline uses the [SortValues](https://beam.apache.org/releases/javadoc/2.14.0/org/apache/beam/sdk/extensions/sorter/SortValues.html) transform provided by the Beam Java SDK.

See the unit test for a usage example.

### Requirements

* Java 8
* Maven 3

### Building the Project

Build the entire project using the maven compile command.
```sh
mvn clean compile
```

### Running the unit test
```sh
mvn clean compile test
```