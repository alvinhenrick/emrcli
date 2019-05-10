def mock_ec2_client(*args, **kwargs):
    class TestEc2Client:
        @classmethod
        def get_ami_id(cls, ami_id):
            return "i-0555ggdgff777"

    ec2_client = TestEc2Client()

    return ec2_client
