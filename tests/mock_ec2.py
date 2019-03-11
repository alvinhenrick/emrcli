def mock_ec2_client(*args, **kwargs):
    class TestEc2Client:
        @classmethod
        def get_ami_id(cls, user_config_ami_id):
            return "i-0155ggdgds686"

    ec2_client = TestEc2Client()

    return ec2_client
