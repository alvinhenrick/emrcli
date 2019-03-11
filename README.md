## How to use it ?

### Prerequisite

1. Activate virtual env and `pip install emrcli`
2. Run this command `emrcli init dev`
3. Change directory to above `cd emcli_dev`.
6. Update your the `userconfgi.yaml` inside folder. Please reference a template below.

    ```yaml
        ---
        ENV: dev
        aws_profile: dev
        job_name: test_cluster
        user: alvin
        key_pair: alvin
        release_label: emr-5.21.0
        
        instance_type: m4.4xlarge
        instance_count: 2
        instance_volume_size : 100
        spot_bid_price : .40
        timeout_duration_in_minutes: 5
        block_duration_in_minutes: 120
        request_master_on_demand: True
        request_core_on_demand: False
        enable_fleet: False
        
        cores_per_executor: 5
        auto_terminate: True
        
        configuration: configurations.json
        bootstrapaction: bootstrap-actions.json
        steps: step.json
        
        pem_file_path: /path/to/dev.pem
        script_to_run: test.sh
        
        start_date: 2019-01-02

        tags:
          -
            Key : "User"
            Value : "Test"
          -
            Key : "Description"
            Value : "Test Pipeline"
        
        custom_vars:
          var1: "value1" 
          var2: "value2"
          var3: "value3"      
   
    ```

7. You can update `configurations.json` file.
8. You can update `steps.json` file.
9. You can update `bootstrap-actions.json` file.

### How to run the pipelines on EMR Cluster and test them.

1. Run these commands as per your need.The parameters inside **square brackets** are optional.`--profile` is your aws profile

* EMR Cli Help
    
    ```bash
       emrcli --help
    ```
* EMR Cli Command Help
    
    ```bash
       emrcli create --help
        
       emrcli submit-steps --help
        
       emrcli terminate --help
        
       emrcli create-with-steps --help
        
       emrcli tunnel --help
        
       emrcli ssh --help
        
       emrcli script-runner --help
        
       emrcli install --help
    ```
* Start EMR Cluster
    
    ```bash
       emrcli create [--profile dev]
    ```
* Submit steps to EMR Cluster.
    
    ```bash
       emrcli submit-steps [--profile dev] \
       [-c|--cid j-1V9ZR35NDKX87] \
       [-d|--date 2018-06-02] \
       [--step_name step1,step3, | --step_idx 1:3] \
       [--custom_vars "key1=val1,key2=value2"]
    ```
* Terminate the EMR Cluster.
    
    ```bash
       emrcli terminate [--profile dev] \
       [-c|--cid j-1V9ZR35NDKX87]
    ```
* Start EMR Cluster and Submit Steps with **Prompt** to keep it alive.
    
    ```bash
       emrcli create-with-steps [--profile dev] \
       [-d|--date 2018-06-02] \
       [--step_name step1,step3,... | --step_idx 1:3] \
       [--custom_vars "key1=val1,key2=value2"]
    ```
* Start EMR Cluster and Submit Steps and keeps it alive **No Prompt**.

    ```bash
       emrcli create-with-steps [--profile dev] \
       [--keep_alive] \
       [-d|--date 2018-06-02] \
       [--step_name step1,step3, | --step_idx 1:3] \
       [--custom_vars "key1=val1,key2=value2"]
    ```
* Create the the ssh tunnel to master node in Cluster.
    
    ```bash
       emrcli tunnel [--profile dev] \
       [-c|--cid j-1V9ZR35NDKX87] \
       [-p|--pem /path/to/key.pem]
    ```
* SSH into master node of Cluster.
    
    ```bash
       emrcli ssh [--profile dev] \
       [-c|--cid j-1V9ZR35NDKX87] \
       [-p|--pem /path/to/key.pem]
    ```
* Runs shell script on all the nodes of cluster.
    
    ```bash
       emrcli script-runner [--profile dev] \
       [-c|--cid j-1V9ZR35NDKX87] \
       [-s|--script test.sh] \
       [-p|--pem /path/to/key.pem] \
       [-q|--quiet] 
    ```
* Build and deploy the local pyspark code.
   
    ```bash
       emrcli install [--profile dev] \
       [-c|--cid j-1V9ZR35NDKX87] \
       [-p|--pem /path/to/key.pem]
    ```

