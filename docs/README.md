---
marp: true
theme: uncover
paginate: true
_paginate: skip
---


<style scoped>
section {
  text-align: center;
}
</style>

**--DRAFT--**

![w:700 center](https://upload.wikimedia.org/wikipedia/commons/9/95/Apache_Iceberg_Logo.svg)
# Evaluation
[MoJ Analytical Platform](https://user-guidance.analytical-platform.service.justice.gov.uk/#content)
[GitHub Repository](https://github.com/moj-analytical-services/iceberg-evaluation)

---
## Data Pipeline Architecture As-Is

1. [AWS DMS](https://aws.amazon.com/dms/) for extracting full loads and changed data (cdc)
2. [AWS Glue PySpark](https://docs.aws.amazon.com/glue/latest/dg/spark_and_pyspark.html) for creating curated tables and orchestrated using [Step Functions](https://aws.amazon.com/step-functions/)
3. [Amazon Athena](https://www.amazonaws.cn/en/athena/) for creating derived tables and orchestrated using [DBT](https://www.getdbt.com/) via [create-a-derived-table](https://github.com/moj-analytical-services/create-a-derived-table)
4. Data stored in [S3](https://aws.amazon.com/s3/) and metadata in [Glue Data Catalog](https://towardsaws.com/data-cataloging-in-aws-glue-c649fa5be715)

![alt architecture](architecture_existing.drawio.png)
<sup>See [Managed Pipelines](https://ministryofjustice.github.io/analytical-platform-data-engineering/) for more details</sup>

---
## Data Pipeline Architecture To-Be

<sup>Option 1: Convert curated tables to [Iceberg](https://iceberg.apache.org/) table format<sup>
![architecture_proposed_pyspark](architecture_proposed_pyspark.drawio.png)

<sup>Option 2: Migrate curation to Athena + DBT in conjunction with Iceberg<sup> 
![architecture_proposed ](architecture_proposed.drawio.png)

---
## Evaluation Outcome

1. Using Iceberg simplifies the curation process 
2. Iceberg is compatible with the data engineering tech stack
2. Out-of-the-box, Athena + Iceberg is cheaper and more performant for our use cases than Glue PySpark + Iceberg

**Hence we are proceeding with option 2**

This also has the advantage of unifying the data engineering tech stack which facilitates collaboration across the data engineering community of practice and minimizes duplication


---
<style scoped>
section {
  text-align: center;
}
</style>
# Background Concepts

---
## Data Curation

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
2. Uses complex process for handling [data shuffling](https://medium.com/distributed-computing-with-ray/executing-a-distributed-shuffle-without-a-mapreduce-system-d5856379426c) which makes it hard to maintain/debug 
3. Large volumes of intermittent missing data and duplicates, but given the complexity of the current job, the root-cause could not be identified

Could we improve performance and simplify PySpark job by making use of Iceberg?

---
## Data Lake Table Formats

- [Table formats](https://www.dremio.com/blog/comparison-of-data-lake-table-formats-apache-iceberg-apache-hudi-and-delta-lake/) abstract groups of data files as a single "table" so we can treat data lakes like databases
- [Apache Hive](https://hive.apache.org/), the original table format, defines a table as all the files in one or more particular directories
- [Modern table format](https://www.dremio.com/blog/comparison-of-data-lake-table-formats-apache-iceberg-apache-hudi-and-delta-lake/) ([Apache Hudi](https://hudi.apache.org/), Databrick's [Delta Lake](https://delta.io/), and Apache Iceberg) store additional metadata 
- Allows query engines to identify relevant data files -> minimise data scans and speed up queries

---
## Acid Transactions

Row level changes!

-
-
-

---
## Why Apache Iceberg?

Comparison of table formats:
1. Performance is very dependent on [optimisation](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-transparent-tpc-ds-lakehouse-performance-benchmarks)
2. Community support is [comparable](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison)
2. [Ecosystem support](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison):

|Ecosystem|Hudi|Delta Lake|Iceberg|
|-|-|-|-|
|AWS Glue|Read+Write|Read+Write|Read+Write|
|Athena|Read|Read|Read+Write|

---
## Why Amazon Athena?

Athena engine version 3 provides better integration with:
- AWS Glue Data Catalog
- Apache Iceberg table format

This means it is now possible to use Athena to process jobs previously not possible. This has many advantages:

1. Costs based on amount of data scanned ($5/TB)
2. Let Amazon Athena determine optimum cluster settings
3. Unified tech stack across data pipeline

---
<style scoped>
section {
  text-align: center;
}
</style>
# How did we evaluate [Apache Iceberg](https://iceberg.apache.org/) for our use cases?

---
## Evaluation Criteria

1. Compatibility:
2. Running Cost:
3. Complexity/Readability:
3. Running Time:


---
## TPC-DS Benchmark

- [TPC-DS](https://www.tpc.org/tpcds/default5.asp) is an industry benchmark consisting of: 
  - 25 tables whose total size can vary (1GB to 100TB) 
  - 99 queries ranging from simple aggregations to advanced pattern analysis
- AWS used the TPC-DS 1TB `stores_sales` table to demonstrate how Hudi can speed up [bulk inserts and upserts](https://aws.amazon.com/blogs/big-data/part-1-get-started-with-apache-hudi-using-aws-glue-by-implementing-key-design-concepts/) 
- AWS used the TPC-DS queries to show Athena 3 can increase query performance
- [TPC-DS connector for AWS Glue](https://aws.amazon.com/marketplace/pp/prodview-xtty6azr4xgey) generates TPC-DS datasets

---
<style scoped>
section {
   margin: 0em 0em 0em 0em;
   padding: 0px 10px 0px 0px;
}
</style>

![bg left:60% 80%](architecture_evaluation.drawio.png)
#### Data Curation evaluation

Compare cost and execution time of bulk insert and SCD2 for `stores_sales` table:
- Scales: 1GB, 3TB
- Proportion: 0.1, 1, 10, 99%

---
## Bulk Insert comparison

![w:1100 center](bulk_insert_comparison.png)
- Athena is cheaper than PySpark at both scales
- PySpark is faster at larger scales

---
## SCD2 comparison

![w:1100 center](scd2_comparison.png)
- Athena is consistently cheaper and faster than PySpark
- PySpark job at 3TB scale failed at all proportions
- Athena query failed at higher proportions

---
## Code complexity

Spark SQL and Trino SQL essentially identical:


---
## Data Curation conclusions

- Athena+Iceberg can handle cdc volumes of 80 million rows against a table of 8 billion rows
- Our current largest table is ~3 billion rows with daily cdcs of ~1 million
- All done with 0 optimisation, there are many optimisations we could make to further handle larger volumes (e.g. sorting, partitioning, etc...)

---
## Deriving from Iceberg tables

-

---
## Compatibility with tech stack

Data engineering have built various tools and libraries to support data analysis on the MoJ Analytical Platform:
- data-engineering-database-access:
- pydbtools
- Rdbtools

---
<style scoped>
section {
  text-align: center;
}
</style>
# Conclusion, Risks and Roadmap

---
## Conclusions

-
-
-

---
## Risks

- Did not have time to investigate:
  - impact of data skew on write-performance
  - impact of table-width on write-performance
  - updating a table impact read-performance
- Replacing dependency on specialist spark expertise with Iceberg expertise
- The proposed solution might not be able to handle future volumes

---
## Questions to Answer

1. How to improve performance using sorting, partitions, file compaction etc...
2. Estimate maximum volume capacity with these optimisations in place
2. How to leverage DBT and [create-a-derived-table](https://github.com/moj-analytical-services/create-a-derived-table)
3. How to monitor code complexity with create-a-derived-table and flag violations


---
## Roadmap

-
-
-

---
## If we had had more time...

- Run TPC-DS 99 queries in spark-SQL to compare performance against Athena
- Terraform codebase to allow collaborators to easily reproduce the results
- Investigate SCD2 failure causes to understand origin

---

# Appendix

---
## Modern glue development options

These modern [glue development options](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html) helped us to develop and test glue pyspark jobs:

- Hello
-





<style>
a,h1,h2 {
  color: #1d70b8;
}
a {
  text-decoration: underline;
}
p {
  font-size:35px;
}
ul, ol {
  margin-left: 0;
  margin-right: 0;
  font-size:35px;
}
section {
  text-align: left;
}
</style>