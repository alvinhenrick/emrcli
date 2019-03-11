import boto3


class Ec2Client(object):

    def __init__(self, aws_profile):
        self.profile_name = aws_profile
        self.ec2 = self.ec2_client(profile_name=self.profile_name)

    @classmethod
    def ec2_client(cls, profile_name=None):

        if profile_name is not None:
            session = boto3.Session(profile_name=profile_name, region_name="us-west-2")
            return session.client("ec2")
        else:
            return boto3.client("ec2")

    def get_ami_id(self, user_config_ami_id):
        if user_config_ami_id is not None:
            return user_config_ami_id
        else:
            return ""
