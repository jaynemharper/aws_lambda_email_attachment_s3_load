# aws_lambda_email_attachment_s3_load

This code extracts attachments from JSON email file, loads attachments to S3 buckets. <br>
<br>
1. Defines variables and S3 resources. <br>
2. Defines lambda_handler() function that accesses S3 bucket holding JSON email file, extracts attachments, uploads resulting files to S3 buckets. <br>
