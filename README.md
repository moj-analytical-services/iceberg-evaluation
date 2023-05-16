# Hudi vs Iceberg

## Context

[Managed Pipelines](https://ministryofjustice.github.io/analytical-platform-data-engineering/) pulls data from various MOJ legacy/heritage databases to the [Analytical Platform](https://user-guidance.services.alpha.mojanalytics.xyz/), MoJ's data analysis environment, providing modern tools and key datasets for MoJ analysts.

Managed Pipelines uses an AWS Glue PySpark job to curate the data through a daily batch ETL process:

- Identify which records where deleted since the last upload, in case the extraction step is unable to determine on-going changes.
- Implement a [Type 2 Slowly Changing Dimension (SCD2)](https://en.wikipedia.org/wiki/Slowly_changing_dimension) to retain the full history of data. When a row is updated or deleted on the source database, the current record on the AP is "closed" and a new record is inserted with the changed data values.

The team tried to use Athena initially but found it couldn't handle these operations at the required scale.

The performance of the AWS Glue PySpark job has been degrading over the last few months, with monthly costs tripling.

Insert diagram of costs??

The Apache Hudi and Iceberg frameworks for data lakes can simplify and enhance incremental data processing (Note that Delta Lake by Databricks is also available but more compatible with the Databricks environment and hence disreguarded). AWS has also recently released [Athena version 3.0](https://aws.amazon.com/about-aws/whats-new/2022/10/amazon-athena-announces-upgraded-query-engine/) with performance improvements. We would like to improve the Managed Pipeline curation step to make use of these advances. This repository investigates the performance improvements and limitations of using different combinations of glue/athena with hudi/iceberg. Many articles already compare Hudi and Iceberg e.g. https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison. However we wanted to compare according to use cases relevant to the MoJ Analytical Platform.

## Use Cases

The tables below summarise the different uses cases and combinations of technologies they were compared against. For more details, please refer to the individual links.

The use cases have been split up by purpose to help organise the findings, but there is some overlap between them.

### Data Engineering

Data Engineering use cases focus on versioning, deduping and uploading the data to the Analytical Platform.

It's not possible to write to a Hudi table with Athena, hence its exclusion from the options below.

|Use Case|Glue+Hudi|Glue+Iceberg|Athena+Iceberg|
|-|:-:|:-:|:-:|
|Bulk Insert|:warning: <br />Slow|:white_check_mark: <br />Completed twice faster|:warning: <br />Slow|
|SCD2|||
|Difference|||
|Deduplication|||
|Schema evolution (on Write)|||

The following use cases are out of scope:

- Streaming _ The Analytical Platform does not use streaming data as yet which makes it difficult to come up with a use case
- Concurrency _ This is only important when there are frequent updates to the data, which is not currently the case with the Analytical Platform
- Partition evolution _ The curated data is partioned by curation date, if at all

### Data Modelling

Data Modelling uses cases focus on transforming and denormalising the data.

Whilst it is possible to use glue to transform data, it is overly complicated for the use cases below.

|Use Case|Athena+Hudi|Athena+Iceberg|
|-|:-:|:-:|
|Select|:x: <br />Not Possible|:white_check_mark: <br />Possible|
|Aggregation|||
|Join|||
|Schema evolution (on Read)|||
|Partition evolution|||

### Data Analysis

Data analysis uses cases focus on querying and analysing the data once it has been transformed and denormalised.

Whilst it is possible to use glue for analysis, it is overly complicated for the use cases below.

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
- Performance _ Number of data processing units (DPUs) used
- Time _ Time it takes for job/query to complete
- Cost _ Cost in $
- Complexity _ Number of lines of code

**Optimizations:**

- Partitioning / File compaction
- Multiple indexing
