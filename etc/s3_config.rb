require 'aws-sdk'

S3_BUCKET_NAME = 'change-blog-datascience'

AWS.config({
  secret_access_key: ENV['AWS_SECRET_ACCESS_KEY'],
  access_key_id:     ENV['AWS_ACCESS_KEY_ID'],
  max_retries: 2,
  s3_endpoint:  's3-us-west-2.amazonaws.com', # US West Oregon supports read-after-write consistency and costs the same as US Standard,
})
