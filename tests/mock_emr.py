from datetime import datetime


def mock_emr_client(*args, **kwargs):
    class TestEmrClient:
        @classmethod
        def run_job_flow(cls, **kwargs):
            return {"ResponseMetadata": {"HTTPHeaders": {"content-length": "1624",
                                                         "content-type": "application/x-amz-json-1.1",
                                                         "date": "Mon, 02 Jul 2018 23:44:46 GMT",
                                                         "x-amzn-requestid": "e7371e56-7e51-11e8-b253-4754b23ad985"},
                                         "HTTPStatusCode": 200,
                                         "RequestId": "e7371e56-7e51-11e8-b253-4754b23ad985",
                                         "RetryAttempts": 0}, "JobFlowId": "s-SNGBtR34"}

        @classmethod
        def terminate_job_flows(cls, **kwargs):
            return {"ResponseMetadata": {"HTTPHeaders": {"content-length": "1624",
                                                         "content-type": "application/x-amz-json-1.1",
                                                         "date": "Mon, 02 Jul 2018 23:44:46 GMT",
                                                         "x-amzn-requestid": "e7371e56-7e51-11e8-b253-4754b23ad985"},
                                         "HTTPStatusCode": 200,
                                         "RequestId": "e7371e56-7e51-11e8-b253-4754b23ad985",
                                         "RetryAttempts": 0}}

        @classmethod
        def describe_cluster(cls, **kwargs):
            return {"ResponseMetadata": {"HTTPHeaders": {"content-length": "1624",
                                                         "content-type": "application/x-amz-json-1.1",
                                                         "date": "Mon, 02 Jul 2018 23:44:46 GMT",
                                                         "x-amzn-requestid": "e7371e56-7e51-11e8-b253-4754b23ad985"},
                                         "HTTPStatusCode": 200,
                                         "RequestId": "e7371e56-7e51-11e8-b253-4754b23ad985",
                                         "RetryAttempts": 0},
                    'Cluster': {
                        'Id': 's-SNGBtR34l',
                        'Name': 'test_cluster',
                        'Status': {
                            'State': 'RUNNING',
                            'Timeline': {
                                'CreationDateTime': datetime(2018, 1, 1),
                                'ReadyDateTime': datetime(2018, 1, 1),
                                'EndDateTime': datetime(2018, 1, 1)
                            }
                        },
                        'InstanceCollectionType': 'INSTANCE_GROUP',
                        'MasterPublicDnsName': 'ip-10-22-182-88',
                    }
                    }

        @classmethod
        def list_instance_fleets(cls, **kwargs):
            return {"ResponseMetadata": {"HTTPHeaders": {"content-length": "1624",
                                                         "content-type": "application/x-amz-json-1.1",
                                                         "date": "Mon, 02 Jul 2018 23:44:46 GMT",
                                                         "x-amzn-requestid": "e7371e56-7e51-11e8-b253-4754b23ad985"},
                                         "HTTPStatusCode": 200,
                                         "RequestId": "e7371e56-7e51-11e8-b253-4754b23ad985",
                                         "RetryAttempts": 0},
                    'InstanceFleets': [
                        {
                            'Id': 'id1',
                            'Name': 'Master',
                            'Status': {
                                'State': 'RUNNING',
                                'Timeline': {
                                    'CreationDateTime': datetime(2018, 1, 1),
                                    'ReadyDateTime': datetime(2018, 1, 1),
                                    'EndDateTime': datetime(2018, 1, 1)
                                }
                            },
                            'InstanceFleetType': 'MASTER',
                            'TargetOnDemandCapacity': 1,
                            'ProvisionedOnDemandCapacity': 1,
                            'InstanceTypeSpecifications': [
                                {
                                    'InstanceType': 'm4.4xlarge',
                                    'WeightedCapacity': 100,
                                    'BidPrice': '0.40',
                                    'BidPriceAsPercentageOfOnDemandPrice': 100.0,
                                    'Configurations': [
                                        {
                                            'Classification': 'spark-defaults',
                                            'Properties': {
                                                'spark.ssl.ui.enabled': 'false',
                                                'spark.authenticate.secret': 'foo'
                                            },
                                            'Configurations': []
                                        },
                                        {
                                            'Classification': 'yarn-site',
                                            'Properties': {
                                                'yarn.resourcemanager.am.max-attempts': '1'
                                            },
                                            'Configurations': []
                                        },
                                        {
                                            'Classification': 'core-site',
                                            'Properties': {
                                                'fs.s3.canned.acl': 'BucketOwnerFullControl'
                                            },
                                            'Configurations': []
                                        }
                                    ],
                                    'EbsBlockDevices': [
                                        {
                                            'VolumeSpecification': {
                                                'VolumeType': 'gp2',
                                                'SizeInGB': 100
                                            },
                                            'Device': 'string'
                                        },
                                    ],
                                    'EbsOptimized': True | False
                                },
                            ],
                            'LaunchSpecifications': {
                                'SpotSpecification': {
                                    'TimeoutDurationMinutes': 5,
                                    'TimeoutAction': 'SWITCH_TO_ON_DEMAND',
                                    'BlockDurationMinutes': 120
                                }
                            }
                        },
                        {
                            'Id': 'id2',
                            'Name': 'Core',
                            'Status': {
                                'State': 'RUNNING',
                                'Timeline': {
                                    'CreationDateTime': datetime(2018, 1, 1),
                                    'ReadyDateTime': datetime(2018, 1, 1),
                                    'EndDateTime': datetime(2018, 1, 1)
                                }
                            },
                            'InstanceFleetType': 'CORE',
                            'TargetSpotCapacity': 2,
                            'ProvisionedSpotCapacity': 2,
                            'InstanceTypeSpecifications': [
                                {
                                    'InstanceType': 'm4.4xlarge',
                                    'WeightedCapacity': 100,
                                    'BidPrice': '0.40',
                                    'BidPriceAsPercentageOfOnDemandPrice': 100.0,
                                    'Configurations': [
                                        {
                                            'Classification': 'spark-defaults',
                                            'Properties': {
                                                'spark.ssl.ui.enabled': 'false',
                                                'spark.authenticate.secret': 'foo'
                                            },
                                            'Configurations': []
                                        },
                                        {
                                            'Classification': 'yarn-site',
                                            'Properties': {
                                                'yarn.resourcemanager.am.max-attempts': '1'
                                            },
                                            'Configurations': []
                                        },
                                        {
                                            'Classification': 'core-site',
                                            'Properties': {
                                                'fs.s3.canned.acl': 'BucketOwnerFullControl'
                                            },
                                            'Configurations': []
                                        }
                                    ],
                                    'EbsBlockDevices': [
                                        {
                                            'VolumeSpecification': {
                                                'VolumeType': 'gp2',
                                                'SizeInGB': 100
                                            },
                                            'Device': 'string'
                                        },
                                    ],
                                    'EbsOptimized': True | False
                                },
                            ],
                            'LaunchSpecifications': {
                                'SpotSpecification': {
                                    'TimeoutDurationMinutes': 5,
                                    'TimeoutAction': 'SWITCH_TO_ON_DEMAND',
                                    'BlockDurationMinutes': 120
                                }
                            }
                        }
                    ]
                    }

        @classmethod
        def list_instance_groups(cls, **kwargs):
            return {"ResponseMetadata": {"HTTPHeaders": {"content-length": "1624",
                                                         "content-type": "application/x-amz-json-1.1",
                                                         "date": "Mon, 02 Jul 2018 23:44:46 GMT",
                                                         "x-amzn-requestid": "e7371e56-7e51-11e8-b253-4754b23ad985"},
                                         "HTTPStatusCode": 200,
                                         "RequestId": "e7371e56-7e51-11e8-b253-4754b23ad985",
                                         "RetryAttempts": 0},
                    'InstanceGroups': [
                        {
                            'Id': 'id1',
                            'Name': 'Master',
                            'Market': 'ON_DEMAND',
                            'InstanceGroupType': 'MASTER',
                            'InstanceType': 'm4.4xlarge',
                            'RequestedInstanceCount': 1,
                            'RunningInstanceCount': 1,
                            'Status': {
                                'State': 'RUNNING',
                                'Timeline': {
                                    'CreationDateTime': datetime(2018, 1, 1),
                                    'ReadyDateTime': datetime(2018, 1, 1),
                                    'EndDateTime': datetime(2018, 1, 1)
                                }
                            },
                            'Configurations': [
                                {
                                    'Classification': 'spark-defaults',
                                    'Properties': {
                                        'spark.ssl.ui.enabled': 'false',
                                        'spark.authenticate.secret': 'foo'
                                    },
                                    'Configurations': []
                                },
                                {
                                    'Classification': 'yarn-site',
                                    'Properties': {
                                        'yarn.resourcemanager.am.max-attempts': '1'
                                    },
                                    'Configurations': []
                                },
                                {
                                    'Classification': 'core-site',
                                    'Properties': {
                                        'fs.s3.canned.acl': 'BucketOwnerFullControl'
                                    },
                                    'Configurations': []
                                }
                            ],
                            'EbsBlockDevices': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp2',
                                        'SizeInGB': 100
                                    },
                                    'Device': 'string'
                                },
                            ],
                            'EbsOptimized': False,
                        },
                        {
                            'Id': 'id2',
                            'Name': 'Core',
                            'Market': 'SPOT',
                            'InstanceGroupType': 'CORE',
                            'BidPrice': '0.40',
                            'InstanceType': 'm4.4xlarge',
                            'RequestedInstanceCount': 2,
                            'RunningInstanceCount': 2,
                            'Status': {
                                'State': 'RUNNING',
                                'Timeline': {
                                    'CreationDateTime': datetime(2018, 1, 1),
                                    'ReadyDateTime': datetime(2018, 1, 1),
                                    'EndDateTime': datetime(2018, 1, 1)
                                }
                            },
                            'Configurations': [
                                {
                                    'Classification': 'spark-defaults',
                                    'Properties': {
                                        'spark.ssl.ui.enabled': 'false',
                                        'spark.authenticate.secret': 'foo'
                                    },
                                    'Configurations': []
                                },
                                {
                                    'Classification': 'yarn-site',
                                    'Properties': {
                                        'yarn.resourcemanager.am.max-attempts': '1'
                                    },
                                    'Configurations': []
                                },
                                {
                                    'Classification': 'core-site',
                                    'Properties': {
                                        'fs.s3.canned.acl': 'BucketOwnerFullControl'
                                    },
                                    'Configurations': []
                                }
                            ],
                            'EbsBlockDevices': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp2',
                                        'SizeInGB': 100
                                    },
                                    'Device': 'string'
                                },
                            ],
                            'EbsOptimized': False,
                        },

                    ]
                    }

        @classmethod
        def add_job_flow_steps(cls, **kwargs):
            return {"ResponseMetadata": {"HTTPHeaders": {"content-length": "1624",
                                                         "content-type": "application/x-amz-json-1.1",
                                                         "date": "Mon, 02 Jul 2018 23:44:46 GMT",
                                                         "x-amzn-requestid": "e7371e56-7e51-11e8-b253-4754b23ad985"},
                                         "HTTPStatusCode": 200,
                                         "RequestId": "e7371e56-7e51-11e8-b253-4754b23ad985",
                                         "RetryAttempts": 0},

                    'StepIds': [
                        'stepId1',
                        'stepId2'
                    ]
                    }

        @classmethod
        def list_instances(cls, **kwargs):
            return {"ResponseMetadata": {"HTTPHeaders": {"content-length": "1624",
                                                         "content-type": "application/x-amz-json-1.1",
                                                         "date": "Mon, 02 Jul 2018 23:44:46 GMT",
                                                         "x-amzn-requestid": "e7371e56-7e51-11e8-b253-4754b23ad985"},
                                         "HTTPStatusCode": 200,
                                         "RequestId": "e7371e56-7e51-11e8-b253-4754b23ad985",
                                         "RetryAttempts": 0},

                    'Instances': [
                        {'Id': 'ci-3SC4IQXMO1PSK',
                         'Ec2InstanceId': 'i-0576b968542aa508f',
                         'PublicDnsName': '',
                         'PrivateDnsName': 'ip-10-223-183-7.ec2.internal',
                         'PrivateIpAddress': '10.223.183.7',
                         'Status': {'State': 'TERMINATED',
                                    'StateChangeReason': {'Code': 'INTERNAL_ERROR',
                                                          'Message': 'Startup script failed.'},
                                    'Timeline': {'CreationDateTime': datetime(2018, 10, 9, 10),
                                                 'EndDateTime': datetime(2018, 10, 9)}},
                         'InstanceGroupId': 'ig-3CVLDUSAEVB33',
                         'Market': 'ON_DEMAND',
                         'InstanceType': 'm4.4xlarge',
                         'EbsVolumes': [{'Device': '/dev/sdb',
                                         'VolumeId': 'vol-035e6d6d63fd5b244'}]},
                        {'Id': 'ci-31W8Z97DPKISH',
                         'Ec2InstanceId': 'i-0f8a49595746081c6',
                         'PublicDnsName': '',
                         'PrivateDnsName': 'ip-10-223-181-246.ec2.internal',
                         'PrivateIpAddress': '10.223.181.246',
                         'Status': {'State': 'RUNNING',
                                    'StateChangeReason': {},
                                    'Timeline': {'CreationDateTime': datetime(2018, 10, 9),
                                                 'ReadyDateTime': datetime(2018, 10, 9)}},
                         'InstanceGroupId': 'ig-3CVLDUSAEVB33',
                         'Market': 'ON_DEMAND',
                         'InstanceType': 'm4.4xlarge',
                         'EbsVolumes': [{'Device': '/dev/sdb',
                                         'VolumeId': 'vol-024a827a0dfb1f020'}]},
                        {'Id': 'ci-LALR90A040LE',
                         'Ec2InstanceId': 'i-010785f4dd01291c6',
                         'PublicDnsName': '',
                         'PrivateDnsName': 'ip-10-223-182-177.ec2.internal',
                         'PrivateIpAddress': '10.223.182.177',
                         'Status': {'State': 'RUNNING',
                                    'StateChangeReason': {},
                                    'Timeline': {'CreationDateTime': datetime(2018, 10, 9, 10),
                                                 'ReadyDateTime': datetime(2018, 10, 9)}},
                         'InstanceGroupId': 'ig-3CVLDUSAEVB33',
                         'Market': 'ON_DEMAND',
                         'InstanceType': 'm4.4xlarge',
                         'EbsVolumes': [{'Device': '/dev/sdb',
                                         'VolumeId': 'vol-0dfa83f26dba6d166'}]},
                        {'Id': 'ci-7EGA48KCGEPB',
                         'Ec2InstanceId': 'i-00165a7ff705de729',
                         'PublicDnsName': '',
                         'PrivateDnsName': 'ip-10-223-183-233.ec2.internal',
                         'PrivateIpAddress': '10.223.183.233',
                         'Status': {'State': 'RUNNING',
                                    'StateChangeReason': {},
                                    'Timeline': {'CreationDateTime': datetime(2018, 10, 9),
                                                 'ReadyDateTime': datetime(2018, 10, 9)}},
                         'InstanceGroupId': 'ig-3CVLDUSAEVB33',
                         'Market': 'ON_DEMAND',
                         'InstanceType': 'm4.4xlarge',
                         'EbsVolumes': [{'Device': '/dev/sdb',
                                         'VolumeId': 'vol-0fad27754481ed35f'}]},
                        {'Id': 'ci-2HFLSDMDWGQTO',
                         'Ec2InstanceId': 'i-0217e49225744ce71',
                         'PublicDnsName': '',
                         'PrivateDnsName': 'ip-10-223-180-81.ec2.internal',
                         'PrivateIpAddress': '10.223.180.81',
                         'Status': {'State': 'RUNNING',
                                    'StateChangeReason': {},
                                    'Timeline': {'CreationDateTime': datetime(2018, 10, 9),
                                                 'ReadyDateTime': datetime(2018, 10, 9)}},
                         'InstanceGroupId': 'ig-3CVLDUSAEVB33',
                         'Market': 'ON_DEMAND',
                         'InstanceType': 'm4.4xlarge',
                         'EbsVolumes': [{'Device': '/dev/sdb',
                                         'VolumeId': 'vol-00c62a333a2e2bbcf'}]},
                        {'Id': 'ci-26MIX2MMXOOY7',
                         'Ec2InstanceId': 'i-0938b90515b0b8adf',
                         'PublicDnsName': '',
                         'PrivateDnsName': 'ip-10-223-182-250.ec2.internal',
                         'PrivateIpAddress': '10.223.182.250',
                         'Status': {'State': 'RUNNING',
                                    'StateChangeReason': {},
                                    'Timeline': {
                                        'CreationDateTime': datetime(2018, 10, 9),
                                        'ReadyDateTime': datetime(2018, 10, 9)}},
                         'InstanceGroupId': 'ig-3FR4STYY3V56R',
                         'Market': 'ON_DEMAND',
                         'InstanceType': 'm4.xlarge',
                         'EbsVolumes': [{'Device': '/dev/sdb',
                                         'VolumeId': 'vol-051e3cb1c47348904'}]},
                        {'Id': 'ci-29QIUQ3NYBVG6',
                         'Ec2InstanceId': 'i-011513d5f9721926b',
                         'PublicDnsName': '',
                         'PrivateDnsName': 'ip-10-223-181-179.ec2.internal',
                         'PrivateIpAddress': '10.223.181.179',
                         'Status': {'State': 'RUNNING',
                                    'StateChangeReason': {},
                                    'Timeline': {'CreationDateTime': datetime(2018, 10, 9),
                                                 'ReadyDateTime': datetime(2018, 10, 9)}},
                         'InstanceGroupId': 'ig-3CVLDUSAEVB33',
                         'Market': 'ON_DEMAND',
                         'InstanceType': 'm4.4xlarge',
                         'EbsVolumes': [{'Device': '/dev/sdb',
                                         'VolumeId': 'vol-090f8c3caac1a9ca4'}]}]
                    }

    emr_client = TestEmrClient()
    return emr_client
