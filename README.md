# Week 1 - Class 1


# Week 1 - Class 2

# Week 1 - Class 3

# Week 2 - Class 1

# Week 2 - Class 2

# Week 2 - Class 3


# Week 3 - Class 1- EMR Serverless

## Create Roles
1. Create **EMR Notebook Role**
- Open **IAM** and create the IAM role for the EMR notebook using the [emr notebook role json](code/week3/config/emr_notebook_rol_priv.json)

```
{
    "Version": "2008-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "Service": "elasticmapreduce.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

```

- Attach **AmazonElasticMapReduceEditorsRole** policy
- Attached **AmazonS3FullAccess** policy 

2. Create **EMR Servlerless Execution Role**
- Open **IAM** and create the IAM role for the EMR Servlerless Execution using [emr serverless role](code/week3/config/emr_serverless_role_priv.json)

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "emr-serverless.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

```

- Attach [policy for permisions](code/week3/config/emr_serverless_policy.json)

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ReadAccessForEMRSamples",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::*.elasticmapreduce",
                "arn:aws:s3:::*.elasticmapreduce/*"
            ]
        },
        {
            "Sid": "FullAccessToOutputBucket",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::*",
                "arn:aws:s3:::*/*"
            ]
        },
        {
            "Sid": "GlueCreateAndReadDataCatalog",
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:CreateDatabase",
                "glue:GetDataBases",
                "glue:CreateTable",
                "glue:GetTable",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "glue:GetTables",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:CreatePartition",
                "glue:BatchCreatePartition",
                "glue:GetUserDefinedFunctions"
            ],
            "Resource": [
                "*"
            ]
        }
    ]
}

```

## Create S3 Buckets

3. Create a new S3 bucket
- Open S3 console 
- create S3 bucket to use for this class

4. Create Folders to use in S3 Bucket
- Create a `pyspark` folder
- Create a `hive` folder
- Create a `datasets` folder (We use this to upload a CSV to)
- Create a `outputs` folder
- Create a `results` folder
- Upload files to folders


## EMR Studio

1. Naviagte to EMR home from the AWS Console and select EMR Studio from the left handside.

2. Select `Get Started`

3. Select `Create Studio`

4. Insert Studio name

5. Under `Networking and Security` select your default VPC and 3 public subnets.

6. Select the EMR Studio role `emr-notebook-role` created initially

7. Select the S3 bucket created initially.

8. Select the `Studio access URL`

## Spark App

9. Select `applications` under `serverless` from the left handside menu

10. Select `create application` from the top right

11. Enter a name for the Application. Leave the type as `Spark` and click `create application`

12. Click into the application via the `name`

13. Click `submit job`

14. Name job and select the service role created in the set up steps.

15. Click `Submit Job`

16. job status will go from pending -> running -> (success or failed).

## Hive App

17. Create Application from applications

18. Name and select Hive application

19. Open hive application

20. Submit the job

21. Name the hive job, select hive script (change bucket name in script),and select service role.

22. Copy and paste Hive config (change bucket name in json).

23. Submit Job and monintor. Job status will go from pending -> running -> success.

24. Navigate to Glue databases and click emrdb 

25. Check the table created

26. Select data using AWS Athena and check the created table.

# Week 3 - Class 2 - Dataframes


# Week 3 - Class 3 - Project - Data Modelling and Planning


## dataset: 
 <a href="https://data.iowa.gov/Sales-Distribution/Iowa-Liquor-Sales/m3tr-qhgy">Iowa-Liquor-Sales</a>

## Dataset exploration results
### columns :
- 'Invoice/Item Number',
- 'Date', 
- 'Store Number',
- 'Store Name',
- 'Address',
- 'City'
- 'Zip Code'
- 'Store Location'
- 'County Number'
- 'County'
- 'Category',
- 'Category Name'
- 'Vendor Number'
- 'Vendor Name',
- 'Item Number'
- 'Item Description'
- 'Pack',
- 'Bottle Volume (ml)'
- 'State Bottle Cost',
- 'State Bottle Retail'
- 'Bottles Sold',
- 'Sale (Dollars)',
- 'Volume Sold (Liters)'
- 'Volume Sold (Gallons)'

### Tables
- Dimensions (COUNTY, ITEMS, STORE, VENDOR, DATES, CATEGORY)
- Facts (sales)

## DATA MODEL (SnowFlake Schema)

![image](https://user-images.githubusercontent.com/8701464/195738436-2f13aef0-0f0b-4e84-ad1c-7ed0379b5092.png)

## ETL Plan

- Create a new schema for the large csv dataset using **StructType** y **StructField**
- Read the .csv file from S3, and load the dataset using a dataframe using **.Cache()** or **.Persist()** with the already defined schema.
- Be careful with date columns and columns with currency symbols
- Write 6 queries in order to create 6 **DIMENSIONS** tables using the dataframe already persisted.
- Write a query in order to create the fact table: **FACT**, the query will use the dataframe already persited.
- Additionally you could add another job that works as check data quality to verify the data
- After this exercise please delete the glue catalog tables, delete the created workgroup, delete the applications in the emr-studio, delete the s3 bucket folders and delete the created roles.
