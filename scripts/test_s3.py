import luigi
from luigi.contrib.s3 import S3Client, S3Target

# used this link below,
# along with searching how to create an S3 bucket using aws cli...
# which I had previously installed on my VM using AWS getting started guide
# (creating_s3_bucket.sh script for command line script to create bucket)

# https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/create-bucket.html


# along with this question...
# https://stackoverflow.com/questions/51081960/how-to-write-a-pickle-file-to-s3-as-a-result-of-a-luigi-task

#
#Configuration: * You must have boto installed. * You must have a [s3] section in your client.cfg with values for aws_access_key_id and aws_secret_access_key (IAM credentials should also be supported, but I haven't tried).

# from following blog site.
# https://www.crobak.org/2014/09/luigi-aws/

# and finally piecing it all together by looking at the source code for the luigi.contrib.s3 module....

# I arrived at how to upload an object to my S3 Bucket!!!

class Test(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return S3Target("S3://bjj-entities/testing/testing.txt")

    def run(self):
        """
        Testing to see if I can put locally created file into S3 bucket.
        see luigi.cfg file to understand how client connects to S3
        """
        with open('testing.txt', 'w') as fh:
            fh.write("Hello S3 Bucket World!")
        S3Client().put("testing.txt", "S3://bjj-entities/testing/testing.txt")


if __name__ == "__main__":
    luigi.run()
