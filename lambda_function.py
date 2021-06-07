import json
import boto3
import os
from datetime import date
from io import StringIO
import csv
import pandas as pd


sns = boto3.client('sns')

sqsclient = boto3.client('sqs')


def lambda_handler(event, context):

    # print(event)
    # Create an S3 client
    # s3 = boto3.client('s3')
    
    s3 = boto3.client('s3')

    today = str(date.today())
    
    s3_obj = boto3.resource('s3')
    
    count = 0
    
    header = []
    
    logs = []
    
    report_bucket = 'trainingusbatch'
    log_bucket = 'trainingusbatch'
    #For writing log to buffer
    csv_buffer = StringIO()
    csv_io = csv.writer(csv_buffer)
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    archreq_filename = key
    
    # getting the archive json input file 
    fname = s3.get_object(Bucket=bucket, Key=key)
    
    # print(key)
    try:
        # TODO: write code...
        infile = fname['Body'].read().decode('utf-8') #decode('windows-1252')
    except:
        # print('we were unable to decode the file')
        snsresponse = sns.publish(
            TopicArn='arn:aws:sns:us-east-1:657907747545:SMSTopic',   
            Message='we were unable to decode the file '+ key,   
        )
        print(snsresponse)
    
    archreq_json=json.loads(infile)
    
    batchid = archreq_json['ArchiveRequest']['BatchID']
    req_date = archreq_json['ArchiveRequest']['RequestDate']
    
    log_filename = batchid + today + "_archive.csv"
    
    # header = str("Requestdate") + str("     ") + str("Current SC") +("    ") + str("Batchid") + str("     ") + str('PathId') + str("     ") + str("key_prefix") +  str("    ") + str("Archived Class") + str("      Status Code")
    
    # print all the paths and count number of files
    
    
    # fieldnames = ["Requestdate","Current SC","Batchid","PathId","key_prefix","Archived Class","Status Code"]
    
    
    for paths in archreq_json['ArchiveRequest']['ToArchive']['i']:
        
        # print(paths['PathId'])
        
        key = paths['File_URL']
        key_split = key.split('//', 1)
        key_split = key_split[1]
        
        key_split = key_split.split('/', 1)
        
        key_prefix = key_split[1]
      
        # count = count + 1
        
        # print(req_date, batchid, paths['PathId'], key_prefix )
        try:
            bc_sc = ""
            key = s3_obj.Object(report_bucket, key_prefix)
            bc_sc = key.storage_class
            print(bc_sc)
        except:
            print('key not found')
            logline = dict(Requestdate= req_date, currentsc = "Key not found", batch_id = batchid, path = paths['PathId'],key_prefix = key_prefix )
            logs.append(logline)
            continue
            # bc_sc = ""
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
        if bc_sc == "DEEP_ARCHIVE":
            logline = dict(Requestdate= req_date, currentsc = "Already Archived", batch_id = batchid, path = paths['PathId'],key_prefix = key_prefix )
            # print(logline)
            logs.append(logline)
    
        else:
            # #########   S3 copy code     ###########
            copy_source = {
                'Bucket': report_bucket,
                'Key': key_prefix,
            }
            ExtraArgs = {
                'StorageClass': 'DEEP_ARCHIVE',
                'MetadataDirective': 'COPY'
            }
            print("--KeyPrefix")
            print(key_prefix)
            try:
                s3_obj.meta.client.copy(copy_source, report_bucket, key_prefix, ExtraArgs)
                
                logline = dict(Requestdate= req_date, currentsc = bc_sc, batch_id = batchid, path = paths['PathId'],key_prefix = key_prefix )
                logs.append(logline)
            except:
                print("key not found")
                # Need to update key not found in the logs 
                logline = dict(Requestdate= req_date, currentsc = "Key not found", batch_id = batchid, path = paths['PathId'],key_prefix = key_prefix )
                logs.append(logline)
            # key = s3.Object(BUCKET, key_prefix)
        count = count + 1
    # csv_buffer = logs
    # print(csv_buffer)
  
        
    dataFrame = pd.DataFrame(data=logs, columns = ['Requestdate','currentsc','batch_id','path','key_prefix'])
    
    print(dataFrame)
  
    dataFrame.to_csv(csv_buffer)
    

    s3_resource = boto3.resource('s3')
    s3_resource.Object(log_bucket, log_filename).put(Body=csv_buffer.getvalue())
    
    print('archreq_filename')
    # Copy Input file to archreq_processed folder
    archreq_inkey = archreq_filename.split('/', 1)
    copy_source = {
                'Bucket': 'archrequest',
                'Key': archreq_filename,
            }
    # archreq_inkey = archreq_filename.split('/', 1)
    print(archreq_inkey[1])
    processed_key = 'arch_request' + '/' + 'processed_arch_req' + '/' + archreq_inkey[1]
    print(processed_key)
    
    s3_resource.meta.client.copy(copy_source, 'archrequest', processed_key)
    
    # try:
    #     s3.meta.client.copy(copy_source, 'archrequest', processed_key)
    # except:
    #     print("key not found")