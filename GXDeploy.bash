https://docs.greatexpectations.io/docs/deployment_patterns/how_to_use_gx_with_aws/how_to_use_gx_with_aws_using_redshift

# PART 1
aws --version
aws sts get-caller-identity

python3 --version
python -m venv my_venv
python3 -m venv my_venv
source my_venv/bin/activate
python3 -m ensurepip --upgrade

python3 -m pip install boto3
python3 -m pip install great_expectations
great_expectations --version

pip install sqlalchemy sqlalchemy-redshift psycopg2

# or if on macOS:
pip install sqlalchemy sqlalchemy-redshift psycopg2-binary

# Create your Data Context​
import great_expectations as gx
context = gx.data_context.FileDataContext.create(full_path_to_project_directory)

# Configure your Expectations Store on Amazon S3​
great_expectations.yml 

"""
stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/

expectations_store_name: expectations_store

"""

# Update your configuration file to include a new Store for Expectations on Amazon S3​
great_expectations.yml

"""
stores:
  expectations_S3_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleS3StoreBackend
      bucket: <your>
      prefix: <your>

expectations_store_name: expectations_S3_store

"""

"""
class_name: ExpectationsStore
store_backend:
  class_name: TupleS3StoreBackend
  bucket: '<your_s3_bucket_name>'
  prefix: '<your_s3_bucket_folder_name>'
  boto3_options:
    endpoint_url: ${S3_ENDPOINT} # Uses the S3_ENDPOINT environment variable to determine which endpoint to use.
    region_name: '<your_aws_region_name>'

"""

"""
class_name: ExpectationsStore
store_backend:
  class_name: TupleS3StoreBackend
  bucket: '<your_s3_bucket_name>'
  prefix: '<your_s3_bucket_folder_name>'
  boto3_options:
    aws_access_key_id: ${AWS_ACCESS_KEY_ID} # Uses the AWS_ACCESS_KEY_ID environment variable to get aws_access_key_id.
    aws_secret_access_key: ${AWS_ACCESS_KEY_ID}
    aws_session_token: ${AWS_ACCESS_KEY_ID}

"""

"""
class_name: ExpectationsStore
store_backend:
  class_name: TupleS3StoreBackend
  bucket: '<your_s3_bucket_name>'
  prefix: '<your_s3_bucket_folder_name>'
  boto3_options:
    assume_role_arn: '<your_role_to_assume>'
    region_name: '<your_aws_region_name>'
    assume_role_duration: session_duration_in_seconds

"""

# Configure your Validation Results Store on Amazon S3​
# Identify your Data Context's Validation Results Store

great_expectations.yml

"""
stores:
  validations_store:
    class_name: ValidationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/validations/

validations_store_name: validations_store

"""

# Update your configuration file to include a new Store for Validation Results on Amazon S3
great_expectations.yml

"""
stores:
  validations_S3_store:
    class_name: ValidationsStore
    store_backend:
      class_name: TupleS3StoreBackend
      bucket: <your>
      prefix: <your>

"""


"""
class_name: ValidationsStore
store_backend:
  class_name: TupleS3StoreBackend
  bucket: '<your_s3_bucket_name>'
  prefix: '<your_s3_bucket_folder_name>'
  boto3_options:
    endpoint_url: ${S3_ENDPOINT} # Uses the S3_ENDPOINT environment variable to determine which endpoint to use.
    region_name: '<your_aws_region_name>'

"""


"""
class_name: ValidationsStore
store_backend:
  class_name: TupleS3StoreBackend
  bucket: '<your_s3_bucket_name>'
  prefix: '<your_s3_bucket_folder_name>'
  boto3_options:
    aws_access_key_id: ${AWS_ACCESS_KEY_ID} # Uses the AWS_ACCESS_KEY_ID environment variable to get aws_access_key_id.
    aws_secret_access_key: ${AWS_ACCESS_KEY_ID}
    aws_session_token: ${AWS_ACCESS_KEY_ID}

"""


"""
class_name: ValidationsStore
store_backend:
  class_name: TupleS3StoreBackend
  bucket: '<your_s3_bucket_name>'
  prefix: '<your_s3_bucket_folder_name>'
  boto3_options:
    assume_role_arn: '<your_role_to_assume>'
    region_name: '<your_aws_region_name>'
    assume_role_duration: session_duration_in_seconds

"""


# Configure Data Docs for hosting and sharing from Amazon S3​
# Create an Amazon S3 bucket for your Data Docs​
> aws s3api create-bucket --bucket data-docs.my_org --region us-east-1
{
    "Location": "/data-docs.my_org"
}

# Configure your bucket policy to enable appropriate access​
  {
    "Version": "2012-10-17",
    "Statement": [{
      "Sid": "Allow only based on source IP",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::data-docs.my_org",
        "arn:aws:s3:::data-docs.my_org/*"
      ],
      "Condition": {
        "IpAddress": {
          "aws:SourceIp": [
            "192.168.0.1/32",
            "2001:db8:1234:1234::/64"
          ]
        }
      }
    }
    ]
  }


# Apply the access policy to your Data Docs' Amazon S3 bucket​
> aws s3api put-bucket-policy --bucket data-docs.my_org --policy file://ip-policy.json

# Add a new Amazon S3 site to the data_docs_sites section of your great_expectations.yml​

great_expectations.yml

"""
data_docs_sites:
  local_site:
    class_name: SiteBuilder
    show_how_to_buttons: true
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/local_site/
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
  S3_site:  # this is a user-selected name - you may select your own
    class_name: SiteBuilder
    store_backend:
      class_name: TupleS3StoreBackend
      bucket: <your>
    site_index_builder:
      class_name: DefaultSiteIndexBuilder

"""

# Test that your Data Docs configuration is correct by building the site
context.build_data_docs()





# PART 2: Connect to data
# Instantiate your project's DataContext

import great_expectations as gx
context = gx.data_context.FileDataContext.create(full_path_to_project_directory)

# Determine your connection string​
## For this guide we will use a connection_string like this:
redshift+psycopg2://<USER_NAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?sslmode=<SSLMODE>

# Add Data Source to your DataContext​
datasource_name = "my_redshift_datasource"
connection_string = "redshift+psycopg2://<user_name>:<password>@<host>:<port>/<database>?sslmode=<sslmode>"

# With these two values, we can create our Data Source:
datasource = context.sources.add_or_update_sql(
    name=datasource_name,
    connection_string=connection_string,
)


#  Connect to a specific set of data with a Data Asset​
table_asset = datasource.add_table_asset(name="my_table_asset", table_name="taxi_data")

query_asset = datasource.add_query_asset(
    name="my_query_asset", query="SELECT * from taxi_data"
)

# Test your new Data Source​
request = table_asset.build_batch_request()

context.add_or_update_expectation_suite(expectation_suite_name="test_suite")

validator = context.get_validator(
    batch_request=request, expectation_suite_name="test_suite"
)

print(validator.head())




# PART 3: CREATE EXPECTATIONS
# Prepare a Batch Request, empty Expectation Suite, and Validator​

# Use a Validator to add Expectations to the Expectation Suite​

validator.expect_column_values_to_not_be_null(column="passenger_count")
validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)

# Save the Expectation Suite​
validator.save_expectation_suite(discard_failed_expectations=False)


# PART 4: Validate Data​
# Create and run a Checkpoint​
checkpoint = context.add_or_update_checkpoint(
    name="my_checkpoint",
    validations=[{"batch_request": request, "expectation_suite_name": "test_suite"}],
)

checkpoint_result = checkpoint.run()

# Build and view Data Docs​
context.open_data_docs()