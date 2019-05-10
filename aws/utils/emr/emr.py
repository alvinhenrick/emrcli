import json
import logging
import os
import re
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from string import Template

import boto3
import yaml
from aws.utils.common.instancetype import types
from dateutil import parser
from file_read_backwards import FileReadBackwards
from gevent import joinall
from jinja2 import Environment, FileSystemLoader
from pssh.clients.native import ParallelSSHClient

from aws.utils.ec2.ec2 import Ec2Client


def setup_logging(default_level=logging.INFO):
    """
    Setup logging configuration
    """
    logging.basicConfig(level=default_level)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    my_logger = logging.getLogger('HandleEMRCommands')

    # create file handler which logs even info messages
    fh = RotatingFileHandler('emr_cli.log', maxBytes=20000, backupCount=2)
    fh.setLevel(default_level)
    fh.setFormatter(formatter)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    ch.setFormatter(formatter)

    # add the handlers to logger
    my_logger.addHandler(ch)
    my_logger.addHandler(fh)

    return my_logger


class HandleEMRCommands(object):

    def __init__(self, config, with_steps=False, profile_name=None,
                 overwrite_auto_terminate=False, user_job_flow_id=None):
        self.config, self.env_str = self.render(config)
        self.job_flow_id = user_job_flow_id
        self.with_steps = with_steps

        if profile_name is not None:
            self.aws_profile = profile_name
        elif self.config.get("AwsProfile") is not None:
            self.aws_profile = self.config["AwsProfile"]
        else:
            self.aws_profile = None

        self.emr = self.emr_client(profile_name=self.aws_profile)
        self.ec2_client = self.ec2_client(profile_name=self.aws_profile)

        self.job_name = self.generate_job_name()
        self.executor_cores = self.config['CoresPerExecutor']
        self.bootstrap_actions = self.load_bootstrap_actions()
        self.configuration = self.load_configuration()
        self.keep_job_flow_alive = not (self.config['Cluster']['AutoTerminate'])
        self.overwrite_auto_terminate = overwrite_auto_terminate

    @classmethod
    def render(cls, config):

        # Load data from YAML into Python dictionary
        with open(config) as fp:
            user_config = yaml.load(fp)

        env_str = user_config.get("ENV").lower()
        template = "masteremrconfig.yaml"

        # Load Jinja2 template
        env = Environment(loader=FileSystemLoader(os.path.dirname(__file__)), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template(template)

        # Render the template with data and print the output
        emr_config = yaml.load(template.render(user_config))

        return emr_config, env_str

    @classmethod
    def emr_client(cls, profile_name=None):

        if profile_name is not None:
            session = boto3.Session(profile_name=profile_name, region_name="us-west-2")
            return session.client("emr")
        else:
            return boto3.client("emr")

    @classmethod
    def ec2_client(cls, profile_name=None):
        return Ec2Client(profile_name)

    @classmethod
    def load_json(cls, file_name):
        with open(file_name) as json_data:
            d = json.load(json_data)
        return d

    @classmethod
    def make_slice(cls, expr):
        def to_piece(s):
            return s and int(s) or None

        pieces = list(map(to_piece, expr.split(':')))
        if len(pieces) == 1:
            return slice(pieces[0], pieces[0] + 1)
        else:
            return slice(*pieces)

    def generate_job_name(self):
        return "{}_{}_{}".format(self.config["JobName"].lower(), self.env_str,
                                 datetime.now().strftime("%Y%m%d.%H%M%S.%f"))

    def resolve_date(self, user_date):
        if user_date is not None:
            return parser.parse(user_date)
        config_start_date = self.config['StartDate']
        if config_start_date is not None:
            return parser.parse(str(config_start_date))
        else:
            return datetime.now()

    def resolve_tags(self):
        user_tags = self.config['Tags']
        if user_tags is not None:
            filtered_tag = list(filter(lambda x: x["Key"] != "Name", user_tags))
            default_tags = [
                {
                    'Key': 'Name',
                    'Value': self.job_name
                }
            ]
            filtered_tag += default_tags
            return filtered_tag
        else:
            return [
                {
                    'Key': 'Name',
                    'Value': self.job_name
                },
            ]

    def resolve_custom_vars(self, user_custom_vars):
        if user_custom_vars is not None:
            return user_custom_vars
        custom_vars = self.config['CustomVars']
        if custom_vars is not None:
            if isinstance(custom_vars, dict):
                return custom_vars
            else:
                raise ValueError("The custom_vars can only be of type dictionary")
        else:
            return {}

    def load_bootstrap_actions(self):
        if self.config["BootStrapAction"]["File"] is not None:
            bs_file = self.config["BootStrapAction"]["File"]
            return self.load_json(bs_file)
        else:
            return []

    def load_configuration(self):
        if self.config["Configuration"]["File"] is not None:
            bs_file = self.config["Configuration"]["File"]
            return self.load_json(bs_file)
        else:
            return []

    def load_steps(self, instance_type, instance_count, user_date=None,
                   user_step_name=None, user_step_idx=None, user_custom_vars=None):

        if user_step_name is not None and user_step_idx is not None:
            raise ValueError("You can use --step_name or --step_idx not both.")

        if self.with_steps and self.config["Steps"]["File"] is not None:
            step_file = self.config["Steps"]["File"]
            with open(step_file) as json_data:
                data = json_data.read().replace("\n", "")
            num_executors, executor_cores, executor_memory = self.spark_parameters(instance_type, instance_count)
            start_date = self.resolve_date(user_date)
            template = Template(data)
            mapping = dict(num_executors=num_executors, executor_cores=executor_cores,
                           executor_memory=executor_memory, driver_memory=executor_memory,
                           year=start_date.year, month=start_date.month, day=start_date.day)
            custom_vars = self.resolve_custom_vars(user_custom_vars)
            json_str = template.substitute(mapping, **custom_vars)
            steps = json.loads(json_str)
            if user_step_name is not None:
                step_list = user_step_name.split(",")
                filtered_step = [x for x in steps if x["Name"] in step_list]
                if len(filtered_step) > 0 and len(filtered_step) == len(step_list):
                    return filtered_step
                else:
                    raise ValueError(
                        "The step name {} provided is invalid check steps in file {}".format(
                            user_step_name,
                            step_file))
            elif user_step_idx is not None:
                step_slice = self.make_slice(user_step_idx)
                filtered_step = steps[step_slice]
                if len(filtered_step) > 0:
                    return filtered_step
                else:
                    raise ValueError(
                        "The step idx {} provided is invalid check steps in file {}".format(
                            user_step_idx,
                            step_file))
            else:
                return steps
        else:
            return []

    def spark_parameters(self, instance_type, instance_count):

        isinstance_details = types.get(instance_type)
        if isinstance_details is not None:
            count = instance_count if instance_count > 0 else 1
            cpu = isinstance_details["CPU"]
            ram = isinstance_details["RAM"]

            remain_cpu = cpu - 1
            remain_ram = ram - 1

            num_executors_per_node = remain_cpu / self.executor_cores
            num_executors = (num_executors_per_node * count) - 1

            ram_per_executor = (remain_ram / num_executors_per_node)
            executor_memory = ram_per_executor - ram_per_executor * .1
            logger.info(
                "Spark Parameters num_executors={}#executor_cores={}#executor_memory={}".format(int(num_executors),
                                                                                                int(
                                                                                                    self.executor_cores),
                                                                                                int(executor_memory)))
            return int(num_executors), int(self.executor_cores), int(executor_memory)
        else:
            raise ValueError(
                'Invalid InstanceType:Refer to AWS Ec2 InstanceType documentation :%s"' % self.config["Instance"][
                    "Type"])

    def terminate(self):
        job_flow_id = self.find_job_flow_id()
        response = self.emr.terminate_job_flows(
            JobFlowIds=[
                job_flow_id,
            ]
        )
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response_code == 200:
            logger.info("ClusterId {} Terminated".format(job_flow_id))
        else:
            logger.info("Could not terminate the cluster (status code {})".format(response_code))

        return response

    def fetch_instance_type_with_count(self):
        job_flow_id = self.find_job_flow_id()
        response_desc_cluster = self.emr.describe_cluster(
            ClusterId=job_flow_id
        )
        time.sleep(1)
        status_code = response_desc_cluster['ResponseMetadata']['HTTPStatusCode']
        if status_code == 200:
            state = response_desc_cluster['Cluster']['Status']['State']
            if state in ["WAITING", "RUNNING", "STARTING", "BOOTSTRAPPING"]:
                if response_desc_cluster['Cluster']['InstanceCollectionType'] == "INSTANCE_FLEET":
                    response_inst_fleet = self.emr.list_instance_fleets(ClusterId=job_flow_id)
                    time.sleep(1)
                    status_code = response_inst_fleet['ResponseMetadata']['HTTPStatusCode']
                    if status_code == 200:
                        instance_type = response_inst_fleet["InstanceFleets"][0]["InstanceTypeSpecifications"][0][
                            "InstanceType"]
                        core_details = [x for x in response_inst_fleet["InstanceFleets"] if
                                        x["InstanceFleetType"] == "CORE"]
                        if len(core_details) > 0:
                            if int(core_details[0]["ProvisionedOnDemandCapacity"]) > 0:
                                core_count = int(core_details[0]["ProvisionedOnDemandCapacity"])
                            else:
                                core_count = int(core_details[0]["ProvisionedSpotCapacity"])
                        else:
                            core_count = 1
                        return instance_type, core_count
                    else:
                        raise ValueError(
                            "Check your cluster id {}.Failed to list instance fleets.(status code {})".format(
                                job_flow_id,
                                status_code))
                else:
                    response_inst_group = self.emr.list_instance_groups(ClusterId=job_flow_id)
                    time.sleep(1)
                    status_code = response_inst_group['ResponseMetadata']['HTTPStatusCode']
                    if status_code == 200:
                        instance_type = response_inst_group["InstanceGroups"][0]["InstanceType"]
                        core_details = [x for x in response_inst_group["InstanceGroups"] if
                                        x["InstanceGroupType"] == "CORE"]
                        return instance_type, int(core_details[0]["RunningInstanceCount"]) if len(
                            core_details) > 0 else 1
                    else:
                        raise ValueError(
                            "Check your cluster id {}.Failed to list instance groups.(status code {})".format(
                                job_flow_id,
                                status_code))
            else:
                print(
                    "The steps not submitted.Check your cluster id {}.The current state is ".format(job_flow_id, state))
                exit(1)
        else:
            raise ValueError(
                "Check your cluster id {}.Failed to describe cluster.(status code {})".format(job_flow_id,
                                                                                              status_code))

    def add_steps(self, user_date=None, user_step_name=None, user_step_idx=None, user_custom_vars=None):
        """
        :return:
        """
        job_flow_id = self.find_job_flow_id()
        instance_type, instance_count = self.fetch_instance_type_with_count()
        response = self.emr.add_job_flow_steps(
            JobFlowId=job_flow_id,
            Steps=self.load_steps(instance_type, instance_count, user_date=user_date,
                                  user_step_name=user_step_name, user_step_idx=user_step_idx,
                                  user_custom_vars=user_custom_vars)
        )
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response_code == 200:
            for step_id in response['StepIds']:
                logger.info("Added StepId {} to ClusterId {}".format(step_id, job_flow_id))
        else:
            logger.info("Could not create Step (status code {})".format(response_code))
        time.sleep(1)

        return response

    @staticmethod
    def build():
        file_name = "setup.py"  # file to be searched
        scoring_dir = cur_dir = os.getcwd()  # Dir from where search starts can be replaced with any path

        while True:
            file_list = os.listdir(cur_dir)
            parent_dir = os.path.dirname(cur_dir)
            if file_name in file_list:
                print("setup file exists in: ", cur_dir)
                break
            else:
                if cur_dir == parent_dir:  # if dir is root dir
                    raise ValueError("Unable to build the project. setup.py not found")
                else:
                    cur_dir = parent_dir

        # holds tar file name and location
        tar_file_nl = []
        os.chdir(cur_dir)
        os.system("python setup.py sdist")

        for file_name in os.listdir(cur_dir + '/dist/'):
            if file_name.endswith("tar.gz"):
                tar_file_nl.append(file_name)
                tar_file_nl.append(cur_dir + '/dist/' + file_name)
                break

        os.chdir(scoring_dir)
        print(os.getcwd())
        return tar_file_nl

    def install(self, user_pem=None, quiet_mode=False):
        pem_path = user_pem if user_pem is not None else self.config["PemFilePath"]
        cluster_id = self.find_job_flow_id()
        desc_cluster = self.emr.describe_cluster(ClusterId=cluster_id)
        cluster_state = desc_cluster['Cluster']['Status']['State']
        if cluster_state not in ['WAITING', 'RUNNING']:
            raise ValueError("Cluster is not active")
        tags_list = desc_cluster['Cluster']['Tags']

        fail_check = True
        valid_description = ["env=local"]
        valid_names = ['local']

        for tag in tags_list:
            if 'Description' in tag['Key'] and any(value in tag['Value'] for value in valid_description):
                fail_check = False
                break
            if 'Name' in tag['Key'] and any(name in tag['Value'] for name in valid_names):
                fail_check = False
                break

        if not fail_check:
            print("Cluster tags should contain Key=Name, Value='local']")
            print("Cluster tags should contain Key=Description, Value='env=local']")
            raise ValueError("Error: Local build can not deployed on this cluster {0}".format(cluster_id))

        tar_file_nl = HandleEMRCommands.build()
        tar_file_name = tar_file_nl[0]
        tar_file_location = tar_file_nl[1]

        if pem_path is not None:
            response = self.emr.list_instances(
                ClusterId=cluster_id,
            )
            response_code = response['ResponseMetadata']['HTTPStatusCode']
            if response_code == 200:

                hosts = self.active_instances(response)

                print(hosts)

                client = ParallelSSHClient(hosts, user='hadoop', pkey=pem_path)
                copy_files = client.copy_file(tar_file_location, '/home/hadoop/' + tar_file_name)
                joinall(copy_files, raise_error=True)

                output = client.run_command(
                    "python3 -m pip install --upgrade --no-deps --force-reinstall /home/hadoop/" + tar_file_name,
                    sudo=True)
                for host, host_output in output.items():
                    if quiet_mode:
                        for line in host_output.stderr:
                            print(line)
                    else:
                        for line in host_output.stdout:
                            print(line)
                print("Deployed to all nodes")

        return

    def script_runner(self, user_pem=None, user_script_name=None, quiet_mode=False):
        """
        :return:
        """
        script_name = user_script_name if user_script_name is not None else self.config["ScriptToRun"]["File"]
        pem_path = user_pem if user_pem is not None else self.config["PemFilePath"]

        if script_name is not None:
            if pem_path is not None:
                job_flow_id = self.find_job_flow_id()
                response = self.emr.list_instances(
                    ClusterId=job_flow_id,
                )
                response_code = response['ResponseMetadata']['HTTPStatusCode']
                if response_code == 200:

                    hosts = self.active_instances(response)

                    print(hosts)

                    client = ParallelSSHClient(hosts, user='hadoop', pkey=pem_path)

                    if script_name.startswith("/"):
                        # handle absolute path
                        to_script_name = "/home/hadoop/{}".format(os.path.basename(script_name))
                        from_script_name = script_name
                    else:
                        # handle relative path
                        to_script_name = "/home/hadoop/{}".format(script_name)
                        from_script_name = os.path.join(os.getcwd(), script_name)

                    logger.info("Copying script {} to {}".format(from_script_name, to_script_name))

                    copy_files = client.copy_file(from_script_name, to_script_name)
                    joinall(copy_files, raise_error=True)

                    logger.info("Finished copying script {} to {}".format(from_script_name, to_script_name))

                    logger.info("Running script {}".format(to_script_name))

                    output = client.run_command(
                        "chmod +x {} && {}".format(to_script_name, to_script_name), sudo=True)

                    for host, host_output in output.items():
                        if quiet_mode:
                            for line in host_output.stderr:
                                print(line)
                        else:
                            for line in host_output.stdout:
                                print(line)

                    logger.info("Finished script {}".format(to_script_name))

                    return hosts

                else:
                    raise ValueError("Could not list instances (status code {})".format(response))
            else:
                raise ValueError(
                    'pem_file_path is not specified in emrcliconfig_inst_fleets.yaml "pem_file_path:%s"' % pem_path)
        else:
            raise ValueError("script runner shell script not specified")

    @staticmethod
    def active_instances(response):
        hosts = []

        for instance in response['Instances']:
            if instance["Status"]["State"] not in ["STARTING", "TERMINATED"]:
                # Not allowed to run a script in any instance that is starting or terminated
                hosts.append(instance['PrivateIpAddress'])
        return hosts

    def _ssh(self, tunnel=False, user_pem=None):
        pem_path = user_pem if user_pem is not None else self.config["PemFilePath"]
        job_flow_id = self.find_job_flow_id()
        if pem_path is not None:
            response = self.emr.describe_cluster(
                ClusterId=job_flow_id,
            )
            response_code = response['ResponseMetadata']['HTTPStatusCode']
            if response_code == 200:
                master_public_dns_name = response['Cluster']['MasterPublicDnsName']
                match = re.match(r'^(.*)-([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)(.*)$', master_public_dns_name)
                if match:
                    host = "{}.{}.{}.{}".format(match.group(2), match.group(3), match.group(4), match.group(5))
                else:
                    raise ValueError('invalid response "master public dns name:%s"' % master_public_dns_name)
            else:
                raise ValueError('invalid response from aws : %s"' % response)

            ssh_args = ['ssh',
                        '-i', pem_path,
                        '-o', 'StrictHostKeyChecking=no',
                        '-o', 'UserKnownHostsFile=/dev/null']
            if tunnel:
                ssh_args = ssh_args + ['-ND', '8157']

            os.execv('/usr/bin/ssh', ssh_args + ['hadoop@' + host])
        else:
            raise ValueError(
                'pem_file_path is not specified in emrcliconfig_inst_fleets.yaml "pem_file_path:%s"' % pem_path)

    def tunnel(self, user_pem=None):
        self._ssh(tunnel=True, user_pem=user_pem)
        return True

    def ssh(self, user_pem=None):
        self._ssh(tunnel=False, user_pem=user_pem)
        return True

    def find_job_flow_id(self):
        if self.job_flow_id is None:
            with FileReadBackwards("emr_cli.log", encoding="utf-8") as log_file:
                for line in log_file:
                    match = re.search(r'.*JobFlowId=(.*)', line)
                    if match:
                        self.job_flow_id = match.group(1)
                        return self.job_flow_id
        else:
            return self.job_flow_id

    def build_instance_groups(self):
        instance_groups = []

        # Configure Master
        master_group = {
            "InstanceCount": 1,
            "EbsConfiguration": {
                "EbsBlockDeviceConfigs": [
                    {
                        "VolumeSpecification": {
                            "SizeInGB": self.config["Instance"]["VolumeSize"],
                            "VolumeType": "gp2"
                        },
                        "VolumesPerInstance": 1
                    }
                ],
                "EbsOptimized": False
            },
            "InstanceRole": "MASTER",
            "InstanceType": self.config["Instance"]["Type"],
            "Name": "Master",
        }

        # Update with spot price
        if not self.config["Instance"]["MasterOnDemand"]:
            master_group.update({
                'Market': 'SPOT',
                'BidPrice': str(
                    self.config["Instance"]["SpotBidPrice"] if self.config["Instance"]["SpotBidPrice"] > 0 else 0.4)
            })

        instance_groups.append(master_group)

        # Configure Cores
        if self.config["Instance"]["Count"] > 0:
            core_group = {
                "InstanceCount": self.config["Instance"]["Count"],
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "SizeInGB": self.config["Instance"]["VolumeSize"],
                                "VolumeType": "gp2"
                            },
                            "VolumesPerInstance": 1
                        }
                    ],
                    "EbsOptimized": False
                },
                "InstanceRole": "CORE",
                "InstanceType": self.config["Instance"]["Type"],
                "Name": "Core",
            }

            # Update with spot price
            if not self.config["Instance"]["CoreOnDemand"]:
                core_group.update({
                    'Market': 'SPOT',
                    'BidPrice': str(
                        self.config["Instance"]["SpotBidPrice"] if self.config["Instance"]["SpotBidPrice"] > 0 else 0.4)
                })

            instance_groups.append(core_group)

        return instance_groups

    def build_instance_fleet(self):
        instance_fleets = []
        # Configure Master
        if self.config["Instance"]["MasterOnDemand"]:
            instance_fleets.append({
                "Name": "Master",
                "InstanceFleetType": "MASTER",
                "TargetOnDemandCapacity": 1,
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": self.config["Instance"]["Type"],
                        "EbsConfiguration": {
                            "EbsBlockDeviceConfigs": [
                                {
                                    "VolumeSpecification": {
                                        "SizeInGB": self.config["Instance"]["VolumeSize"],
                                        "VolumeType": "gp2"
                                    },
                                    "VolumesPerInstance": 1
                                }
                            ],
                            "EbsOptimized": False
                        }
                    },
                ]
            })
        else:
            instance_fleets.append({
                "Name": "Master",
                "InstanceFleetType": "MASTER",
                "TargetSpotCapacity": 1,
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": self.config["Instance"]["Type"],
                        "BidPrice": str(self.config["Instance"]["SpotBidPrice"]),
                        "EbsConfiguration": {
                            "EbsBlockDeviceConfigs": [
                                {
                                    "VolumeSpecification": {
                                        "SizeInGB": self.config["Instance"]["VolumeSize"],
                                        "VolumeType": "gp2"
                                    },
                                    "VolumesPerInstance": 1
                                }
                            ],
                            "EbsOptimized": False
                        }
                    },
                ],
                "LaunchSpecifications": {
                    "SpotSpecification": {
                        "TimeoutDurationMinutes": self.config["Instance"]["TimeoutDurationMinutes"],
                        "TimeoutAction": "SWITCH_TO_ON_DEMAND",
                        "BlockDurationMinutes": self.config["Instance"]["BlockDurationMinutes"]
                    }
                }
            })
        # Configure Cores
        if self.config["Instance"]["Count"] > 0:
            if self.config["Instance"]["CoreOnDemand"]:
                instance_fleets.append({
                    "Name": "Core",
                    "InstanceFleetType": "CORE",
                    "TargetOnDemandCapacity": self.config["Instance"]["Count"],
                    "InstanceTypeConfigs": [
                        {
                            "InstanceType": self.config["Instance"]["Type"],
                            "EbsConfiguration": {
                                "EbsBlockDeviceConfigs": [
                                    {
                                        "VolumeSpecification": {
                                            "SizeInGB": self.config["Instance"]["VolumeSize"],
                                            "VolumeType": "gp2"
                                        },
                                        "VolumesPerInstance": 1
                                    }
                                ],
                                "EbsOptimized": False
                            }
                        },
                    ]
                })
            else:
                instance_fleets.append({
                    "Name": "Core",
                    "InstanceFleetType": "CORE",
                    "TargetSpotCapacity": self.config["Instance"]["Count"],
                    "InstanceTypeConfigs": [
                        {
                            "InstanceType": self.config["Instance"]["Type"],
                            "BidPrice": str(self.config["Instance"]["SpotBidPrice"]),
                            "EbsConfiguration": {
                                "EbsBlockDeviceConfigs": [
                                    {
                                        "VolumeSpecification": {
                                            "SizeInGB": self.config["Instance"]["VolumeSize"],
                                            "VolumeType": "gp2"
                                        },
                                        "VolumesPerInstance": 1
                                    }
                                ],
                                "EbsOptimized": False
                            }
                        },
                    ],
                    "LaunchSpecifications": {
                        "SpotSpecification": {
                            "TimeoutDurationMinutes": self.config["Instance"]["TimeoutDurationMinutes"],
                            "TimeoutAction": "SWITCH_TO_ON_DEMAND",
                            "BlockDurationMinutes": self.config["Instance"]["BlockDurationMinutes"]
                        }
                    }
                })

        return instance_fleets

    def build_instances(self, user_keep_job_flow_alive):
        instances = {
            "Ec2KeyName": self.config["Ec2attributes"]["KeyName"],
            "Ec2SubnetId": self.config["Ec2attributes"]["SubnetId"],
            "EmrManagedSlaveSecurityGroup": self.config["Ec2attributes"]["EmrManagedSlaveSecurityGroup"],
            "EmrManagedMasterSecurityGroup": self.config["Ec2attributes"]["EmrManagedMasterSecurityGroup"],
            "AdditionalMasterSecurityGroups": self.config["Ec2attributes"]["AdditionalMasterSecurityGroups"],
            "AdditionalSlaveSecurityGroups": self.config["Ec2attributes"]["AdditionalSlaveSecurityGroups"],
            "KeepJobFlowAliveWhenNoSteps": user_keep_job_flow_alive
        }

        if self.config["Instance"]["EnableFleet"]:
            instances["InstanceFleets"] = self.build_instance_fleet()
        else:
            instances["InstanceGroups"] = self.build_instance_groups()

        if self.config["Ec2attributes"]["ServiceAccessSecurityGroup"] is not None:
            instances["ServiceAccessSecurityGroup"] = self.config["Ec2attributes"]["ServiceAccessSecurityGroup"]

        return instances

    def create(self, keep_alive=False, user_date=None,
               user_step_name=None, user_step_idx=None,
               user_custom_vars=None):
        if self.overwrite_auto_terminate:
            user_keep_job_flow_alive = keep_alive
        else:
            user_keep_job_flow_alive = self.keep_job_flow_alive

        resolved_ami_id = self.ec2_client.get_ami_id(self.config["CustomAmiId"])

        security_config = self.config["SecurityConfiguration"]

        response = self.emr.run_job_flow(
            Name=self.job_name,
            CustomAmiId=resolved_ami_id,
            LogUri=self.config["LogUri"],
            ReleaseLabel=self.config["ReleaseLabel"],
            Instances=self.build_instances(user_keep_job_flow_alive),
            Applications=self.config["Applications"],
            ServiceRole=self.config["ServiceRole"],
            JobFlowRole=self.config["Ec2attributes"]["InstanceProfile"],
            SecurityConfiguration=security_config if security_config is not None else "",
            BootstrapActions=self.bootstrap_actions,
            Configurations=self.configuration,
            Tags=self.resolve_tags(),
            VisibleToAllUsers=True,
            Steps=self.load_steps(self.config["Instance"]["Type"],
                                  self.config["Instance"]["Count"] if self.config["Instance"]["Count"] > 0 else 1,
                                  user_date=user_date,
                                  user_step_name=user_step_name, user_step_idx=user_step_idx,
                                  user_custom_vars=user_custom_vars)
        )
        # Process response to determine if Spark cluster was started, and if so, the JobFlowId of the cluster
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response_code == 200:
            self.job_flow_id = response["JobFlowId"]
            logger.info("Created EMR cluster with JobName={} JobFlowId={}".format(self.job_name, self.job_flow_id))
        else:
            logger.info("Could not create EMR cluster (status code {})".format(response_code))
        time.sleep(1)

        return response


logger = setup_logging()
