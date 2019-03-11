emrcliconfig = """
---
ENV: {0} 
aws_profile: default
job_name: test_cluster
user: {1}
key_pair: myuserid
release_label: emr-5.21.0
emr_log_bucket: some-emr-datascience-bucket

instance_type: m4.4xlarge
instance_count: 2
instance_volume_size: 100

cores_per_executor: 3
auto_terminate: True

spot_bid_price: 0.4

request_master_on_demand: True
request_core_on_demand: True

configuration: configurations.json
bootstrapaction: bootstrap-actions.json
steps: steps.json

pem_file_path: /path/to/default.pem
script_to_run: test.sh

tags:
  - Key: "User"
    Value: "{1}"

  - Key: "Description"
    Value: "Data Science EMR Cluster"
"""
