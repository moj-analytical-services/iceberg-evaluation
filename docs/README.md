---
marp: true
theme: uncover
paginate: true
_paginate: skip
---
# Evaluation

![bg left:40% 80%](https://upload.wikimedia.org/wikipedia/commons/9/95/Apache_Iceberg_Logo.svg)

---
## Overview

1. Why did we decide to investigate [Apache Iceberg](https://iceberg.apache.org/)?
2. How did we evaluate [Apache Iceberg](https://iceberg.apache.org/) for our use cases?
4. Conclusion, risks and roadmap

---

# Why did we investigate [Apache Iceberg](https://iceberg.apache.org/)?

---
## Existing Data Pipeline

1. [AWS DMS](https://aws.amazon.com/dms/) to extract full loads and changed data
2. [AWS Glue PySpark](https://docs.aws.amazon.com/glue/latest/dg/spark_and_pyspark.html) to create curated tables 
3. [Amazon Athena](https://www.amazonaws.cn/en/athena/) + [DBT](https://www.getdbt.com/) to create derived tables
4. Data stored in [S3](https://aws.amazon.com/s3/) and metadata in [Glue Data Catalog](https://towardsaws.com/data-cataloging-in-aws-glue-c649fa5be715)

![architecture center](architecture_existing.drawio.png)
See [Managed Pipelines](https://ministryofjustice.github.io/analytical-platform-data-engineering/) for more details

---
## Data Curation processes

1. Bulk insert full loads
2. Impute any deleted rows prior to additional full loads
3. Remove duplicate data
3. Apply [Type 2 Slowly Changing Dimension (SCD2)](https://en.wikipedia.org/wiki/Slowly_changing_dimension) to track how a row changes over time: 

| id | status | updated_at | valid_from | valid_to |
| -- | ------ | ---------- | ---------- | ------------ |
| 1 | pending | 2019-01-01 | 2019-01-01 | 2019-01-02 |
| 1 | shipped | 2019-01-02 | 2019-01-02 | `null` |

---
## Issues with [Glue PySpark job](https://github.com/ministryofjustice/analytical-platform-data-engineering/blob/main/glue_database/glue_jobs/create_derived_table.py)

1. Performance has degraded over the last few months, with monthly costs quadrupling
2. Very complex process for handling [data shuffling](https://medium.com/distributed-computing-with-ray/executing-a-distributed-shuffle-without-a-mapreduce-system-d5856379426c) which makes it hard to maintain/debug 
3. Large volumes of intermittent missing data and duplicates, but given the complexity of the current job, the root-cause could not be identified
4. Lack of specialist Spark expertise in the team

---
## Data Lake Table Formats

- [Table formats](https://www.dremio.com/blog/comparison-of-data-lake-table-formats-apache-iceberg-apache-hudi-and-delta-lake/) abstract groups of data files as a single "table" so we can treat data lakes like databases
- [Apache Hive](https://hive.apache.org/), the original table format, defines a table as all the files in one or more particular directories
- [Modern table format](https://www.dremio.com/blog/comparison-of-data-lake-table-formats-apache-iceberg-apache-hudi-and-delta-lake/) ([Apache Hudi](https://hudi.apache.org/), Databrick's [Delta Lake](https://delta.io/), and Apache Iceberg) store additional metadata 
- Allows query engines to identify relevant data files -> minimise data scans and speed up queries

---
## Why Apache Iceberg?

1. Performance is very dependent on [optimisation](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-transparent-tpc-ds-lakehouse-performance-benchmarks)
2. [Ecosystem support](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison):

|Ecosystem|Hudi|Delta Lake|Iceberg|
|-|-|-|-|
|AWS Glue|Read+Write|Read+Write|Read+Write|
|[Trino](https://trino.io/)|Read|Read|Read+Write|
|Athena|Read|Read|Read+Write|

*Only Iceberg has write-support for Amazon Athena*

---
## Questions to Answer

1. Can we leverage Apache Iceberg to improve performance and decrease costs?
2. Can we replace Glue PySpark with Amazon Athena to decrease costs and simplify tech stack?
3. What is the impact  on Data Derivation processes?

![architecture_proposed ](architecture_proposed.drawio.png)

---
# How did we evaluate [Apache Iceberg](https://iceberg.apache.org/) for our use cases?

---
## Evaluation Criteria

-
-
-


---
## TPCDS Benchmarking

-
-
-

---
## 

## Data Curation Use Cases

1. Full load Bulk Insert and addition of reference columns

2. "Simple" SCD2 where there is only one update per PK which is more recent than the current record

3. "Complex" SCD2 where there can be multiple updates per PK as well as [late-arriving records]()

---
## Results

---
## Data Derivation Use Cases

---
## Results

-
-
-

---
# Conclusion, Risks and Roadmap

---
## Conclusions

-
-
-

---
## Risks

-
-
-

---
## Questions to Answer

1. How to improve performance using sorting, partitions, file compaction etc...
2. How to leverage DBT and [create-a-derived-table](https://github.com/moj-analytical-services/create-a-derived-table)

---
## Roadmap

-
-
-


<style>
a,h1,h2 {
    color: #1d70b8;
}
a{
    text-decoration: underline;
}
</style>