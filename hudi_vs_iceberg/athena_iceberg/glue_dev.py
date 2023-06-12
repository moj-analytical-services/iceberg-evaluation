import boto3
import os
from time import sleep

remote_script_name = 'athena_iceberg.py' #this is alsu used to derive glue job name

def link(uri, label=None):
    if label is None: 
        label = uri
    parameters = ''

    # OSC 8 ; params ; URI ST <name> OSC 8 ;; ST 
    escape_mask = '\033]8;{};{}\033\\{}\033]8;;\033\\'

    return escape_mask.format(parameters, uri, label)

def save_glue_script(local_script_path = '/Users/william.orr/Developer/Projects/hudi-vs-iceberg/hudi_vs_iceberg/athena_iceberg/athena_functions_glue.py'):
    bucket = 'aws-glue-assets-684969100054-eu-west-1'
    remote_script_folder = 'scripts'
    remote_script_name = 'athena_iceberg.py'
    # Copy script updates to s3
    s3_client = boto3.client('s3')
    s3_client.upload_file(Filename=local_script_path, Bucket=bucket, 
                          Key=remote_script_folder + '/' + remote_script_name)
    

def run_glue_script(scd2_type='simple', use_case='bulk_import'):
    # Run the glue job with required params
    glue_client = boto3.client('glue')
    job_name = remote_script_name.replace('.py', '')
    job_args = {
            '--output_key_base':         'data-engineering-use-cases',
            '--primary_key':             'pk',
            '--enable-job-insights':     'false',
            '--enable-glue-datacatalog': 'true',
            '--job-language':            'python',
            '--table':                   'store_sales',
            '--bucket':                  'sb-test-bucket-ireland',
            '--TempDir':                 's3://aws-glue-assets-684969100054-eu-west-1/temporary/',
            '--scale':                   '1',
            '--proportion':              '0.001',
            '--scd2_type':               scd2_type,
            '--use_case':                use_case
        }
    
    job_run_id = (glue_client.start_job_run(
        JobName=job_name,
        Arguments=job_args)['JobRunId']
    )
    # status = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
    # while glue_client.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']['JobRunState'] != 'SUCCEEDED':
    #     print('Job is running')
    #     sleep(1)

    running_statuses = ['STARTING', 'RUNNING']

    while True:
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        status = response['JobRun']['JobRunState']
        if status in running_statuses:
            execution_time = response['JobRun']['ExecutionTime']
            print(f"Status: {status} - Execution Time: {execution_time} seconds")
            
            if status not in running_statuses:
                break
                
        else:
            break
        
        sleep(2)
    cloudwatch_log_url = link(f'https://eu-west-1.console.aws.amazon.com/cloudwatch/home?region=eu-west-1#logsV2:log-groups/log-group/$252Faws-glue$252Fpython-jobs$252Foutput/log-events/{job_run_id}')
    if status == 'SUCCEEDED':
        print('Job Succeeded, woot!!')
        print(f'Log URL: {cloudwatch_log_url}')
    else:
        error_message = response['JobRun']['ErrorMessage']
        print(f'Job  status: {status} and error message: {error_message}')
        print(f'Log URL: {cloudwatch_log_url}')
# update_run_glue_script()

def print_cwd():
    print(os.getcwd())
# Get the job run id
# check the status of the job

# return stats or error position