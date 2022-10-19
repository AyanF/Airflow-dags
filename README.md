### Airflow-dags
This branch of Airflow-dags contains the Dags useful for interacting with Apache NiFi.

The nifi_trigger_dag is used to trigger any NiFi Processor from Airflow by supplying the processor ID.

This a workflow consisting of Airflow DAGs to automate rule execution for thersholding process with OFBiz

Using Airflow DAGs for thresholding -

Clone these DAGs into  the Airflow > dags folder 
	
Setting up connection
  1. Login to Airflow 
  2. Go to Admin> Connections , Click create on add record 
       i) Set connection type as HTTP
       
      ii) Enter host url in Host
      
     iii) Enter dev_apps in Conn ID
     
      iv) Save the connection

For scheduling daily

  1. Save rules to a JSON file 

      Make the following request -

        Api -  http://localhost:8080/api/v1/dags/rules_toJson/dagRuns

        Type - Post

        Sample payload - 

        ```
        {"1": {"facilityId": ["WH"], "propertyResource": "FTP_EXP_CONFIG", "threshold": "1", "searchPreferenceId": "10002", "JOB_NAME": "RuleNo-1", 
        "SERVICE_NAME": "ftpExportProductThresholdCsv", "SERVICE_COUNT": "0", "SERVICE_TIME": "1658953345000", "jobFields": {"productStoreId": "STORE",
        "systemJobEnumId": "JOB_EXP_PROD_THRSHLD", "maxRecurrenceCount": "-1", "recurrenceTimeZone": "Asia/Kolkata"}, "statusId": "SERVICE_PENDING",
        "systemJobEnumId": "JOB_EXP_PROD_THRSHLD", "ruleId": "1"}, "2": {"facilityId": ["WH"], "propertyResource": "FTP_EXP_CONFIG", "threshold": "1", 
        "searchPreferenceId": "10001", "JOB_NAME": "RuleNo-2", "SERVICE_NAME": "ftpExportProductThresholdCsv", "SERVICE_COUNT": "0", 
        "SERVICE_TIME": "1658953345000", "jobFields": {"productStoreId": "STORE", "systemJobEnumId": "JOB_EXP_PROD_THRSHLD", "maxRecurrenceCount": "-1",
        "recurrenceTimeZone": "Asia/Kolkata"}, "statusId": "SERVICE_PENDING", "systemJobEnumId": "JOB_EXP_PROD_THRSHLD", "ruleId": "2"}, "3": 
        {"facilityId": ["WH"], "propertyResource": "FTP_EXP_CONFIG", "threshold": "1", "searchPreferenceId": "10010", "JOB_NAME": "RuleNo-3",
        "SERVICE_NAME": "ftpExportProductThresholdCsv", "SERVICE_COUNT": "0", "SERVICE_TIME": "1658953345000", "jobFields": {"productStoreId": "STORE",
        "systemJobEnumId": "JOB_EXP_PROD_THRSHLD", "maxRecurrenceCount": "-1", "recurrenceTimeZone": "Asia/Kolkata"}, "statusId": "SERVICE_PENDING",
        "systemJobEnumId": "JOB_EXP_PROD_THRSHLD", "ruleId": "3"}}
        ```

        Authentication - Basic <Your Airflow base64 token>

  2. Save sequence to JSON file 

        Make the following request -

        Api -  http://localhost:8080/api/v1/dags/sequence_toJson/dagRuns

        Type - Post

        Sample payload (3,2,1 are rule IDs)- 

        ```
        {"seq": ["3", "2", "1"]} 
        ```

        Authentication - Basic <Your Airflow base64 token>

Open the DAG execute_rulesDaily.py, Go to task schedule_service , edit the headers parameter and enter your Base 64 token in Authorization 

  3. Trigger DAG to create a new JSON which have all the rules to be executed in sequence and adds a “ruleStatus” parameter

        Make the following request 
		
		    Api -  http://localhost:8080/api/v1/dags/read_rulesDaily/dagRuns

        Type - Post

        Payload - {}

        Authentication - Basic <Your Airflow base64 token>

The read_rulesDaily DAG will run Daily and trigger the execute_rulesDaily DAG which will read the JSON and execute the rules.

Upon successful completion  of the rule it will update the JSON, an api call will run the execute_rulesDaily DAG, it’ll pick up the next pending rule.

For executing rules once

Open the DAG execute_rulesDaily.py, Go to task schedule_service , edit the headers parameter and enter your Base 64 token in Authorization 


1. Make the following request

    Put all the rules in sequence inside JSON payload

    Api -  http://localhost:8080/api/v1/dags/read_rules3/dagRuns

    Type - Post

    Sample Payload - 

```
    {"1": {"facilityId": ["WH"], "propertyResource": "FTP_EXP_CONFIG", "threshold": "1", "searchPreferenceId": "10002", "JOB_NAME": "RuleNo-1",
    "SERVICE_NAME": "ftpExportProductThresholdCsv", "SERVICE_COUNT": "0", "SERVICE_TIME": "1658759400000", "jobFields": {"productStoreId": "STORE", 
    "systemJobEnumId": "JOB_EXP_PROD_THRSHLD", "maxRecurrenceCount": "-1", "recurrenceTimeZone": "Asia/Kolkata"}, "statusId": "SERVICE_PENDING", 
    "systemJobEnumId": "JOB_EXP_PROD_THRSHLD", "ruleStatus": "Completed", "ruleId": "01"}, "2": {"facilityId": ["WH"], "propertyResource": "FTP_EXP_CONFIG", 
    "threshold": "1", "searchPreferenceId": "10001", "JOB_NAME": "RuleNo-2", "SERVICE_NAME": "ftpExportProductThresholdCsv", "SERVICE_COUNT": "0",
    "SERVICE_TIME": "1658759400000", "jobFields": {"productStoreId": "STORE","systemJobEnumId": "JOB_EXP_PROD_THRSHLD", "maxRecurrenceCount": "-1",
    "recurrenceTimeZone": "Asia/Kolkata"}, "statusId": "SERVICE_PENDING", "systemJobEnumId": "JOB_EXP_PROD_THRSHLD", "ruleStatus": "Pending", "ruleId": "02"}, 
    "3": {"facilityId": ["WH"], "propertyResource": "FTP_EXP_CONFIG", "threshold": "1", "searchPreferenceId": "10010", "JOB_NAME": "RuleNo-3", 
    "SERVICE_NAME": "ftpExportProductThresholdCsv", "SERVICE_COUNT": "0", "SERVICE_TIME": "1658759400000", "jobFields": {"productStoreId": "STORE",
    "systemJobEnumId": "JOB_EXP_PROD_THRSHLD", "maxRecurrenceCount": "-1", "recurrenceTimeZone": "Asia/Kolkata"}, "statusId": "SERVICE_PENDING", 
    "systemJobEnumId": "JOB_EXP_PROD_THRSHLD", "ruleStatus": "Pending", "ruleId": "03"}}
```

     Authentication - Basic <Your Airflow base64 token>
	

This read_rules3 DAG will create a JSON file containing all the rules to be executed in sequence, it will trigger execute_rules3 DAG  which will run a pending rule in the JSON file.
Upon successful completion  of the rule it will update the JSON, an api call will run the execute_rulesDaily DAG, it’ll pick up the next pending rule. 


>>>>>>> master
