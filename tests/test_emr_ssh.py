import mock

from aws.utils.emr.emr import HandleEMRCommands
from .mock_ec2 import mock_ec2_client
from .mock_emr import mock_emr_client
from .mock_os import mock_os


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
@mock.patch("os.execv", side_effect=mock_os)
def test_ssh_master(mock_emr_client, mock_ec2_client, mock_os):
    response = HandleEMRCommands("tests/configs/emrcliconfig_inst_groups.yaml", with_steps=True,
                                 profile_name=None).ssh()
    assert response is True


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
@mock.patch("os.execv", side_effect=mock_os)
def test_create_tunnel(mock_emr_client, mock_ec2_client, mock_os):
    response = HandleEMRCommands("tests/configs/emrcliconfig_inst_groups.yaml", with_steps=True,
                                 profile_name=None).tunnel()

    assert response is True
