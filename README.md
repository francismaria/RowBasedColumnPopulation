## Row-based Column Values Population

This repository hosts a simple project presenting how to create a new column in a Spark dataframe based on all the values of each row.

### Usage

###### Input

```
sbt

> run

> test
```
###### Output

```
+---------+-----+------+-----+------------+
|repair_id|brand|engine|tires|valid_record|
+---------+-----+------+-----+------------+
|        1|  BMW|   s4t|  255|         YES|
|        2| Fiat|   3er|  245|         YES|
|        3| Audi|  null| null|          NO|
+---------+-----+------+-----+------------+
```


