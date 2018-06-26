from __future__ import print_function
import email
import zipfile
import os
import gzip
import string
import boto3
import urllib

print('Loading function')

# Define S3 resources
s3 = boto3.client('s3')
s3r = boto3.resource('s3')

# Identify variables
xmlDir = "/tmp/output/"
outputBucket = "INSERT S3 OUTPUT BUCKET"  # Set here for a seperate bucket otherwise it is set to the events bucket
first_outputPrefix = "INSERT S3 OUTPUT BUCKET PREFIX FOR ATTACHMENT 1"  # Should end with /
second_outputPrefix = "INSERT S3 OUTPUT BUCKET PREFIX FOR ATTACHMENT 2"  # Should end with /


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).encode('utf8')

    try:
        # Set outputBucket if required
        if not outputBucket:
            global outputBucket
            outputBucket = bucket

        # Use waiter to ensure the file is persisted
        waiter = s3.get_waiter('object_exists')
        waiter.wait(Bucket=bucket, Key=key)

        response = s3r.Bucket(bucket).Object(key)

        # Read the raw text file into a Email Object
        msg = email.message_from_string(response.get()["Body"].read())

        if len(msg.get_payload()) >= 2:  # Means at least one attachment

            # Create directory for XML files (makes debugging easier)
            if os.path.isdir(xmlDir) == False:
                os.mkdir(xmlDir)

            # Define attachments
            first_attachment = msg.get_payload()[1]
            second_attachment = msg.get_payload()[2]

            # Extract the attachments into /tmp/output
            extract_attachment(first_attachment)
            extract_attachment(second_attachment)

            # Upload the XML files to S3
            upload_resulting_files_to_s3()

        else:
            print("Could not see file/attachment.")

        return 0
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist '
              'and your bucket is in the same region as this '
              'function.'.format(key, bucket))
        raise e
    delete_file(key, bucket)


def extract_attachment(first_attachment):
    # Process filename.zip attachments
    if "x-gzip" in first_attachment.get_content_type():
        contentdisp = string.split(first_attachment.get('Content-Disposition'), '"')
        fname = contentdisp[1].replace('\"', '')
        print(fname)
        open('/tmp/' + contentdisp[1], 'wb').write(first_attachment.get_payload(decode=True))
        # This assumes we have filename.xml.gz, if we get this wrong, we will just
        # ignore the report
        xmlname = fname[:-3]
        print(xmlname)
        open(xmlDir + xmlname, 'wb').write(gzip.open('/tmp/' + contentdisp[1], 'rb').read())

    # Process filename.xlsx attachments
    elif "xml" in first_attachment.get_content_type():
        print("xml")
        contentdisp = string.split(first_attachment.get('Content-Disposition'), '"')
        fname = contentdisp[1].replace('\"', '')
        print(fname)
        open('/tmp/' + contentdisp[1], 'wb').write(first_attachment.get_payload(decode=True))
        # with zipfile.ZipFile('/tmp/attachment.zip', "r") as z:
        # z.extractall(xmlDir)
        open(xmlDir + fname, 'wb').write(open('/tmp/' + contentdisp[1], 'rb').read())

    else:
        print('Skipping ' + first_attachment.get_content_type())


def extract_attachment(second_attachment):
    # Process filename.zip attachments
    if "x-gzip" in second_attachment.get_content_type():
        contentdisp = string.split(second_attachment.get('Content-Disposition'), '"')
        fname = contentdisp[1].replace('\"', '')
        print(fname)
        open('/tmp/' + contentdisp[1], 'wb').write(second_attachment.get_payload(decode=True))
        # This assumes we have filename.xml.gz, if we get this wrong, we will just
        # ignore the report
        xmlname = fname[:-3]
        print(xmlname)
        open(xmlDir + xmlname, 'wb').write(gzip.open('/tmp/' + contentdisp[1], 'rb').read())

    # Process filename.xlsx attachments
    elif "xml" in second_attachment.get_content_type():
        print("xml")
        contentdisp = string.split(second_attachment.get('Content-Disposition'), '"')
        fname = contentdisp[1].replace('\"', '')
        print(fname)
        open('/tmp/' + contentdisp[1], 'wb').write(second_attachment.get_payload(decode=True))
        # with zipfile.ZipFile('/tmp/attachment.zip', "r") as z:
        # z.extractall(xmlDir)
        open(xmlDir + fname, 'wb').write(open('/tmp/' + contentdisp[1], 'rb').read())

    else:
        print('Skipping ' + second_attachment.get_content_type())


def upload_resulting_files_to_s3():
    # Put all XML back into S3
    for fileName in os.listdir(xmlDir):
        if fileName.endswith(".xlsx"):
            print("Uploading: " + fileName)  # File name to upload
            if "appointments" in fileName:
                s3r.meta.client.upload_file(xmlDir + '/' + fileName, outputBucket, first_outputPrefix + fileName)
            elif "redemptions" in fileName:
                s3r.meta.client.upload_file(xmlDir + '/' + fileName, outputBucket, second_outputPrefix + fileName)


# Delete the file in the current bucket
def delete_file(key, bucket):
    s3.delete_object(Bucket=bucket, Key=key)
    print("%s deleted fom %s ") % (key, bucket)