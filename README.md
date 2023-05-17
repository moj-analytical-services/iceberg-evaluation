# Hudi vs Iceberg

## Context

[Managed Pipelines](https://ministryofjustice.github.io/analytical-platform-data-engineering/) pulls full loads and on-going data changes from various MOJ legacy/heritage databases to the [Analytical Platform](https://user-guidance.services.alpha.mojanalytics.xyz/), MoJ's data analysis environment, providing modern tools and key datasets for MoJ analysts.

Managed Pipelines uses an AWS Glue PySpark job to curate the data through a daily batch process:

- CDC changes - Implement a [Type 2 Slowly Changing Dimension (SCD2)](https://en.wikipedia.org/wiki/Slowly_changing_dimension) to retain the full history of data. When a row is updated or deleted on the source database, the current record on the AP is "closed" and a new record is inserted with the changed data values.
- Full load to full load changes - Impute records which where deleted since the last upload, in case the extraction step was unable to determine on-going changes.

The performance of the AWS Glue PySpark job has been degrading over the last few months, with monthly costs tripling. We would like to improve the Managed Pipeline curation step to make use of recent advancements.

The Apache Hudi and Iceberg frameworks for data lakes can simplify and enhance incremental data processing (Note that Delta Lake by Databricks is also available but more compatible with the Databricks environment and hence disreguarded). AWS has recently released [Athena version 3.0](https://aws.amazon.com/about-aws/whats-new/2022/10/amazon-athena-announces-upgraded-query-engine/) with performance improvements. 

This repository investigates the performance improvements and limitations of using different combinations of glue/athena with hudi/iceberg. Many articles already compare Hudi and Iceberg e.g. https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison. However we wanted to compare according to use cases relevant to the MoJ Analytical Platform.

### Current file size
Some analysis was carried out to get an idea of the curent volume of data that we are processing. Total (full load and cdc) file count and file size was collected for the raw history folders of both databases.

![file_size](MPM_raw_hist_top20.png)

## Use Cases

The tables below summarise the different uses cases and combinations of technologies they were compared against. For more details, please refer to the individual links.

![architecture](architecture.drawio.png)

Note that the use cases have been split up by data function to help organise the findings, however there is some overlap between them.

### Data Engineering

Data Engineering use cases focus on versioning, deduping and uploading the data to the Analytical Platform.

It's not possible to write to a Hudi table with Athena, hence its exclusion from the options below.

|Use Case|Glue+Hudi|Glue+Iceberg|Athena+Iceberg|
|-|:-:|:-:|:-:|
|Bulk Insert|:warning: <br />Slow|:white_check_mark: <br />Completed twice faster|:warning: <br />Slow|
|SCD2|||
|Impute deletions|||
|Deduplication|||
|Schema evolution (on Write)|||

The following use cases are out of scope:

- Streaming _ The Analytical Platform does not use streaming data as yet which makes it difficult to come up with a use case
- Concurrency _ This is only important when there are frequent updates to the data, which is not currently the case with the Analytical Platform
- Partition evolution _ The curated data is partioned by curation date, if at all

### Data Modelling

Data Modelling uses cases focus on transforming and denormalising the data.

Whilst it is possible to use glue to transform data, it is overly complicated for the use cases below.
There's no need to repeat any use cases already covered above.

|Use Case|Athena+Hudi|Athena+Iceberg|
|-|:-:|:-:|
|Select|:x: <br />Not Possible|:white_check_mark: <br />Possible|
|Aggregation|||
|Join|||
|Schema evolution (on Read)|||
|Partition evolution|||
|Select during SCD2|||

### Data Analysis

Data analysis uses cases focus on querying and analysing the data once it has been transformed and denormalised.

Whilst it is possible to use glue for analysis, it is overly complicated for the use cases below.
There's no need to repeat any use cases already covered above.

|Use Case|Athena+Hudi|Athena+Iceberg|
|-|:-:|:-:|
|Select|:x: <br />Not Possible|:white_check_mark: <br />Possible|
|Timestamps|||
|Partition evolution|||

### Benchmarks

**Volumes:**
- [1K, 1M, 100M, 10B] rows
- [10, 100] columns

**Criteria:**
- Cost _ Glue/Spark is priced at $0.44 per data processing unit (DPU) per hour. Athena is priced at $5.00 per TB of scanned data.
- Complexity / Readability _ How easy is it to understand what the code is doing? Can a problem be easily fixed?
- Time _ Time it takes for job/query to complete

As these batch processes run over night, time is not as important as cost, although there is a direct relationship between time and cost for Glue/Spark. Complexity / Readability is quite subjective and can't be easily measured. Number of lines of code could be used as a proxy, but there is also a point where shorter code is less readable. 

**Optimizations:**
- Partitioning / File compaction
- Multiple indexing
