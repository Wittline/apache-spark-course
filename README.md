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

4. Create Folder To use in S3 Bucket
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

6. Select the EMR Studio role `emr-notebook-role-tutorial` created initially

7. Select the S3 bucket created initially.

8. Select the `Studio access URL`

## Spark App

9. Select `applications` under `serverless` from the left handside menu

10 Select `create application` from the top right

11. Enter a name for the Application. Leave the type as `Spark` and click `create application`

12. Click into the application via the `name`

13. Click `submit job`

14. Name job and select the service role created in the set up steps.

15. Click `Submit Job`

16. job status will go from pending -> running -> success.

## Hive App

17. Create Application from applications

18. Name and select Hive application

19. Open hive application

20. Submit the job

21. Name the hive job, select hive script (change bucket name in script),and select service role.

22. Copy and paste Hive config (change bucket name in json).

23. Submit Job and monintor. Job status will go from pending -> running -> success.

24. Navigate to Glue databases and click emrdb

25. Look at table created

26. Select data using AWS Athena and check the created table.
