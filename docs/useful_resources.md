# Useful Resources

## Introductory Material

- [Hive Tables and What’s Next for Modern Data Platforms](https://bigdataboutique.com/blog/hive-tables-and-whats-next-for-modern-data-platforms-1xts1m) A high level introduction to Hive tables and issues they face
- [Introduction to Apache Hudi](https://bigdataboutique.com/blog/introduction-to-apache-hudi-c83367) A high level introduction to Hudi tables and DeltaStreamer
- [The Apache Iceberg Open Table Format](https://www.dremio.com/open-source/apache-iceberg/) A high level introduction to Iceberg tables

## Comparisons of Hudi and Iceberg

- [Lakehouse Feature Comparison](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison) very thorough comparison but written by OneHouse which develops Hudi-based solutions
- [Comparison of Data Lake Table Formats](https://www.dremio.com/blog/comparison-of-data-lake-table-formats-apache-iceberg-apache-hudi-and-delta-lake/) Another very thorough comparision but written by Dremio which develops Iceberg-based solutions
- [Comparison of Data Lakehouse Table Formats](https://www.dremio.com/subsurface/subsurface-meetup-comparison-of-data-lakehouse-table-formats/) Explanation of how table formats work, their features, and their limitations
- [Reassessing Performance](https://databeans-blogs.medium.com/delta-vs-iceberg-vs-hudi-reassessing-performance-cb8157005eb0) Performance comparison published by databeans which develops deltalake-based solutions
- [Transparent TPC-DS Lakehouse Performance Benchmarks](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-transparent-tpc-ds-lakehouse-performance-benchmarks) A riposte by OneHouse disputing the results from the previous blog
- [Data Lake / Lakehouse Guide](https://airbyte.com/blog/data-lake-lakehouse-guide-powered-by-table-formats-delta-lake-iceberg-hudi) An overview and comparison of data lakehouse formats written by a neutral party
- [Benchmarking Open Table Formats](https://brooklyndata.co/blog/benchmarking-open-table-formats) Performance comparison of delta lake and iceberg, also by a neutral company


## AWS Interoperability

- [Get started with Apache Hudi using AWS Glue](https://aws.amazon.com/blogs/big-data/part-1-get-started-with-apache-hudi-using-aws-glue-by-implementing-key-design-concepts/) How to get started with Apache Hudi on AWS glue and some available optimizations
- [Native support for Apache Hudi, Delta Lake, and Apache Iceberg on AWS Glue](https://aws.amazon.com/blogs/big-data/part-1-getting-started-introducing-native-support-for-apache-hudi-delta-lake-and-apache-iceberg-on-aws-glue-for-apache-spark/)
- [Using Iceberg and Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
- [AWS workshop demonstrating some of the main functionality of Athena with iceberg](https://catalog.us-east-1.prod.workshops.aws/workshops/9981f1a1-abdc-49b5-8387-cb01d238bb78/en-US/90-athena-acid)
- [Documentation on developing with Glue locally using the official AWS Glue docker image](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-docker-image)
- [Apache Iceberg on AWS with S3 and Athena](https://www.youtube.com/watch?v=iGvj1gjbwl0) 
  - A useful introductory tutorial from Jonny Chivers on how Iceberg tables work and are different to Hive, and how to implement a database with Iceberg tables purely with Athena Presto SQL. 
  - A supporting tutorial is [SQL for Athena](https://www.youtube.com/watch?v=V21xjnHMOyk) where he builds a database on S3 purely with AWS Athena with Presto SQL, but with Hive tables. 
- [https://medium.com/expedia-group-tech/a-short-introduction-to-apache-iceberg-d34f628b6799](https://medium.com/expedia-group-tech/a-short-introduction-to-apache-iceberg-d34f628b6799) A dated but nice high level introduction to Iceberg tables
- Both Hudi and Iceberg are compatible with AWS LakeFormation (including e.g. column and cell level access). See [here](https://docs.aws.amazon.com/lake-formation/latest/dg/otf-tutorial.html) for the official documentation. For a lab that implements LakeFormation permissions for Hudi see [here](https://catalog.us-east-1.prod.workshops.aws/workshops/976050cc-0606-4b23-b49f-ca7b8ac4b153/en-US/1200/1211-hudi-lakeformation-lab) and for Iceberg see [here](https://catalog.us-east-1.prod.workshops.aws/workshops/976050cc-0606-4b23-b49f-ca7b8ac4b153/en-US/1200/1212-iceberg-lakeformation-lab).

## In-depth Resources

- [Apache Iceberg 101](https://www.dremio.com/blog/apache-iceberg-101-your-guide-to-learning-apache-iceberg-concepts-and-practices/) Link to set of videos and additional resources on Apache Iceberg
