﻿# HDInsight Delta Lake Example Code

The project contains two modules:

1. hdi4spark24 - [HDInsight 4.0](https://learn.microsoft.com/en-us/azure/hdinsight/hdinsight-40-component-versioning) with Spark 2.4
2. hdi5spark3 - [HDInsight 5.0](https://learn.microsoft.com/en-us/azure/hdinsight/hdinsight-50-component-versioning) with Spark 3.0

Both modules have an example of Writing, Reading, and Time Travel example code using Delta Lake on the HDInsight platform.

## hdi4spark24

The hdi4spark24 module is based on:

1. HDInsight 4.0
2. JRE 1.8
3. Spark 2.4.2
4. Scala 2.11.12

Main Application class is `com.ms.hdi.spark.deltalake.example.DeltaLakeExample`

You don't have to pass any additional configuration other than Delta Lake  (0.6.1) and jackson (2.6.7) dependencies via the shaded uber jar file. 

## hdi5spark3

The hdi5spark3 module is based on:

1. HDInsight 5.0
2. JRE 11
3. Spark 3.0.2
4. Scala 2.12.11

Main Application class is `com.ms.hdi.spark.deltalake.example.DeltaLakeSpark3Example`

You need to set the following Spark configurations:

- spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
- spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

And Delta Lake (1.0.1) dependency via a shaded uber jar file.

## Example Output

You can validate the output of example code from Yarn logs and Storage output. 

The Yarn stdout will be: 

![](images/yarnlog.png)

And Storage will have a couple of parquet files with delta commit logs (_delta_log).

![](images/storage.png)

You can find examples using Jupyter Notebook (https://github.com/sethiaarun/hdinotebookexamples)
