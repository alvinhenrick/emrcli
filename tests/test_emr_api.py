import mock

from aws.utils.emr.emr import HandleEMRCommands
from .mock_ec2 import mock_ec2_client
from .mock_emr import mock_emr_client


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
def test_create_cluster_instance_groups(mock_emr_client, mock_ec2_client):
    response = HandleEMRCommands("tests/configs/emrcliconfig_inst_groups.yaml", profile_name=None).create()
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response["JobFlowId"] == "s-SNGBtA88"


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
def test_run_with_steps_instance_groups(mock_emr_client, mock_ec2_client):
    response = HandleEMRCommands("tests/configs/emrcliconfig_inst_groups.yaml", with_steps=True,
                                 profile_name=None, overwrite_auto_terminate=True).create()
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response["JobFlowId"] == "s-SNGBtA88"


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
def test_terminate_cluster(mock_emr_client, mock_ec2_client):
    response = HandleEMRCommands("tests/configs/emrcliconfig_inst_groups.yaml", profile_name=None).terminate()
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
def test_add_steps(mock_emr_client, mock_ec2_client):
    response = HandleEMRCommands("tests/configs/emrcliconfig_inst_groups.yaml", profile_name=None).add_steps()
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
def test_create_cluster_instance_fleet(mock_emr_client, mock_ec2_client):
    response = HandleEMRCommands("tests/configs/emrcliconfig_inst_fleets.yaml", profile_name=None).create()
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response["JobFlowId"] == "s-SNGBtA88"


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
def test_run_with_steps_instance_fleet(mock_emr_client, mock_ec2_client):
    response = HandleEMRCommands("tests/configs/emrcliconfig_inst_groups.yaml", with_steps=True,
                                 profile_name=None).create()
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response["JobFlowId"] == "s-SNGBtA88"


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
def test_run_with_steps_selected_by_name(mock_emr_client, mock_ec2_client):
    response = HandleEMRCommands("tests/configs/emrcliconfig_inst_groups.yaml", with_steps=True,
                                 profile_name=None, overwrite_auto_terminate=True).create(
        user_step_name="my_first_job_1,my_first_job_2")
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response["JobFlowId"] == "s-SNGBtA88"


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
def test_add_steps_selected_by_name(mock_emr_client, mock_ec2_client):
    response = HandleEMRCommands("tests/configs/emrcliconfig_inst_groups.yaml", profile_name=None).add_steps(
        user_step_name="Step1,Step2")
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
def test_run_with_steps_selected_by_idx(mock_emr_client, mock_ec2_client):
    response = HandleEMRCommands("tests/configs/emrcliconfig_inst_groups.yaml", with_steps=True,
                                 profile_name=None).create(
        user_step_idx="1:2")
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response["JobFlowId"] == "s-SNGBtA88"


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
def test_add_steps_selected_by_idx(mock_emr_client, mock_ec2_client):
    response = HandleEMRCommands("tests/configs/emrcliconfig_inst_groups.yaml", profile_name=None).add_steps(
        user_step_idx="3:")
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@mock.patch("aws.utils.emr.emr.HandleEMRCommands.emr_client", side_effect=mock_emr_client)
@mock.patch("aws.utils.emr.emr.HandleEMRCommands.ec2_client", side_effect=mock_ec2_client)
@mock.patch("pssh.clients.native.ParallelSSHClient.copy_file")
@mock.patch("pssh.clients.native.ParallelSSHClient.run_command")
def test_script_runner(mock_emr_client, mock_ec2_client, mock_copy_file, mock_run_command):
    hosts = HandleEMRCommands("tests/configs/emrcliconfig_inst_groups.yaml", profile_name=None).script_runner()
    assert len(hosts) == 6
