# Mozart - Business logic for SA360

[TOC]

Mozart is a framework for automating tasks on Search Ads 360 (SA360). Mozart
lets advertisers and agencies apply their own business logic to SA360 campaigns
by leveraging well-known technologies such as Apache Beam.

Mozart is designed to be deployed in an Airflow+Beam platform. The rest of this
documentation assumes Google Cloud Platform (GCP) is used for deployment.
*Composer* is the name of GCP's managed version of *Airflow*, whereas *DataFlow*
is the name of the managed version of *Beam*.

## How it works
Mozart leverages SA360 Reporting API and SA360 Bulksheet uploads to perform the
automation tasks.

The sequence of high-level operations is:

 1. Reports are downloaded from SA360 API. These reports must include all
 entities to be processed (e.g.: Keywords, Ads).
 2. Downloaded reports are analyzed, applying the custom business logic. The
 output of this logic is a CSV file containing updated information about the
 entities (Keywords, Ads). For example: a new Max CPC value for certain keywords
 3. CSV files with updated values are uploaded into SA360 using sFTP Bulksheet
 upload

## Architecture
Mozart consists of two main modules:

 1. An Airflow DAG
 2. A Beam Pipeline
