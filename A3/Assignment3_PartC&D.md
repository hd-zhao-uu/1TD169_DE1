# Assignment 3

---

# Section C

## C.1

***What is RDD (Explain the Resilient and Distributed)? How does Spark work with RDD?***

1. RDD(Resilient Distributed Dataset) is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel and can be rebuilt if a partition is lost.
    - **Resilient:**
        - RDDs are read-only, immutable.
        - RDDs are fault tolerant. They achieve fault tolerance through a notion of **lineage**: if a
        partition of an RDD is lost, the RDD has enough information about how it was derived from other RDDs to be able to rebuild just that partition.
    - **Distributed:** RDDs are distributed across cluster

1. In Spark, RDDs can be created in 2 ways:
    - parallelize an existing collection in your driver program,
    - reference a dataset in an external storage system, e.g. a shared filesystem, HDFS, HBase.

---

## C.2

***Your colleague is trying to understand her Spark code by adding a print statement inside her `split_line(..)` function, as shown in this code snippet:***

```cpp
def split_line(line):
		print('splitting line...')
		return line.split(' ')

lines = spark_context.textFile("hdfs://host:9000/i-have-a-dream.txt")
print(lines.flatMap(split_line).take(10))
```

***When she runs this code in her notebook, she sees the following output:
`['I', 'am', 'happy', 'to', 'join', 'with', 'you', 'today', 'in', 'what']`***

***But, she doesn’t see the “splitting line…” output in her notebook. Why not?***

This code snippet contains driver (main/master) and executors (processes running on a Spark worker node in cluster mode). And the function `split_line()` which runs inside `flatMap()` runs on the executors.

In other words, when in cluster mode, execution print inside `flatMap()` or `map()` function will show in the nodes console. 

---

## C.3

***Calling*** `.collect()` ***on a large dataset may cause driver application to run out of memory Explain why.***

Suppose currently we have a driver and many executors. There is a large dataset(file) that is sitting on multiple executors. 

When we call `.collect()`, this operation will make the driver try to merge all the parts of the file into the driver we use. 

In this situation, the out of memory error might occur if the file exceeds the total available storage in our driver.

---

## C.4

***Are partitions mutable or immutable? Why is this advantageous?***

The partitions are immutable.

- Immutability is the way to go for highly concurrent (multithreaded) systems. If do read and write (update) at the same time,  concurrency is harder to achieve. Immutable data can be shared safely across various processes and threads
- Spark can recompute the RDD from the lineage in case of failure.
- Immutability can enhance the computation process by caching RDD

---

## C.5

***What is the difference of DataFrame and RDD? Explain their advantages/disadvantages.***

***Difference:***

1. RDDs are unstructured, while DataFrames are structured or semi-structured.
2. The model of RDD is set of values, or set of key/value pairs, while the model of DataFrames is table.
3. RDDs are low-level and provide fine control of MapReduce functions, while DataFrames are high-level, more like SQL.
4. When use RDDs, we need to define the schema manually. But Dataframes will automatically find out the schema of the dataset.

***RDD***

**Advantage:** 

- The advantageous part about RDD is simple. It provides familiar OOPs style APIs with compile time safety. We can load any data from a source, convert them into RDD and store in memory to compute results. RDD can be easily cached if same set of data needs to recomputed.

**Disadvantages:**

- **Performance limitations.** Being in-memory JVM objects, RDDs involve overhead of Garbage Collection and Serialization which are expensive when data grows.

<div style="page-break-after: always;"></div>

**DataFrame**

**Advantage:** 

- The huge performance improvement over RDDs because of
    - Custom Memory management
      
        Data is stored in binary format. This saves a lot of memory space. There is no GC(Garbage Collection) and Serialization overhead involved like RDD.
        
    - Optimized Execution Plans.

**Disadvantages:**

- The compiler is not able to catch errors as the code refers to data attribute names. Errors are detected during the run time after the creation of query plans.
- Domain objects cannot be regenerated from it.

---

**Reference:**

1. [http://vishnuviswanath.com/spark_rdd.html](http://vishnuviswanath.com/spark_rdd.html)
2. [https://spark.apache.org/docs/latest/rdd-programming-guide.html](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
3. [https://www.quora.com/What-is-difference-between-dataframe-and-RDD](https://www.quora.com/What-is-difference-between-dataframe-and-RDD)

---

---

<div style="page-break-after: always;"></div>

# Section D

***“A colleague has mentioned her Spark application has poor performance, what is your advice?”***

1. **Cache the RDD**
   
    It’s safe to say that there will be so many intermediate results can be reused in the Spark Application. 
    
    Reading from source is time-consuming. If we use the `cache()` method we can store the RDDs in-memory. The RDD persisted in memory can be used efficiently across parallel operations. 
    
2. **Optimize Query(when use DataFrames)**
   
    This is similar to optimizing SQL queries. It’s not efficient to query the things we need by using the whole table. We can just query on the columns related to the result.  
    
3. **Avoid Long Lineage**
   
    It is not efficient to have so many transformations in a lineage, especially when we need to process huge volumes of data but the resources are limited. 
    
    We can break(split) the chain of lineage by writing intermediate results.  
    
4. **Inefficient or Incorrect Configuration**
   
    Every Spark Application has its own memory and caching requirements. If the applications are inefficient or incorrectly configured, they may slow down or even crash.
    
    We need to pay attention to the memory used by executors and our driver. According the memory used by them, we can get conclusion that if it needs more memory or we can save memory.