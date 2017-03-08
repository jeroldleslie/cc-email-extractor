# Email Extractor from common crawl dataset
This repository contains hadoop mapreduce programe to filter website urls from Common Crawl public datasets.

## Prerequisities
- Need AWS account
- Create awsAccessKeyId and awsSecretAccessKey
- Create S3 bucket
- Create Amazon EMR cluster to run mapreduce

## How to run in Amazon EMR
- Checkout this repository locally
- Import into eclipse as existing maven project
- run `mvn install`
- build the project using `mvn package` or right click pom.xml and run as `maven build`
- Upload the standalone build jar in S3 bucket
- Add mapreduce step in the Amazon EMR cluster with custom jar option. Browse and add jar from S3 bucket
- Add [arguments](#arguments) like explained below
- Run the mapreduce step

### Arguments
It requires 4 arguments to run
- awsAccessKeyId
- awsSecretAccessKey
- Common Crawl WARC S3n path
- mapreduce output path

### Example
To Run all warc files under a s3 directory
```<awsAccessKeyId> <awsSecretAccessKey> s3n://commoncrawl/<segment-path>/*.warc.gz s3n://<bucket-name>/output```

or

Only to run single warc file
```<awsAccessKeyId> <awsSecretAccessKey> s3n://commoncrawl/<segment-path>/xxxxx.warc.gz s3n://<bucket-name>/output```


