import json
import boto3
import os
from datetime import date
from io import StringIO
import csv
import pandas as pd


sns = boto3.client('sns')

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')

    today = str(date.today())
    
    s3_obj = boto3.resource('s3')
    
    count = 0
    
    header = []
    
    logs = []
    
    # Source Bucket Hardcode
    report_bucket = 'trainingusbatch'

    #Bucket for Logs
    log_bucket = 'trainingusbatch'
    
    #For writing log to buffer
    csv_buffer = StringIO()
    csv_io = csv.writer(csv_buffer)
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    archreq_filename = key
    
    # Getting the archive json input file 
    fname = s3.get_object(Bucket=bucket, Key=key)
    
    # Reading the input file
    try:
        infile = fname['Body'].read().decode('utf-8') 
    except:
        # print('we were unable to decode the file')
        snsresponse = sns.publish(
            TopicArn='arn:aws:sns:us-east-1:657907747545:SMSTopic',   
            Message='we were unable to decode the file '+ key,   
        )
    
    archreq_json=json.loads(infile)
    
    #collecting BatchID and Request date from the input file
    batchid = archreq_json['ArchiveRequest']['BatchID']
    req_date = archreq_json['ArchiveRequest']['RequestDate']
    
    #Log File name format
    log_filename = batchid + today + "_log.csv"
    
    #Parse input file for File url, archive files and generate logs
    for paths in archreq_json['ArchiveRequest']['ToArchive']['i']:
        
        key = paths['File_URL']
        key_split = key.split('//', 1)
        key_split = key_split[1]
        
        key_split = key_split.split('/', 1)
        
        key_prefix = key_split[1]
      
        # count = count + 1
        
        try:
            bc_sc = ""
            key = s3_obj.Object(report_bucket, key_prefix)
            bc_sc = key.storage_class
        except:
            print('key not found')
            logline = dict(Requestdate= req_date, currentsc = "Key not found", batch_id = batchid, path = paths['PathId'],key_prefix = key_prefix )
            logs.append(logline)
            continue
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
        if bc_sc == "DEEP_ARCHIVE":
            logline = dict(Requestdate= req_date, currentsc = "Already Archived", batch_id = batchid, path = paths['PathId'],key_prefix = key_prefix )
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
            try:
                s3_obj.meta.client.copy(copy_source, report_bucket, key_prefix, ExtraArgs)
                
                logline = dict(Requestdate= req_date, currentsc = bc_sc, batch_id = batchid, path = paths['PathId'],key_prefix = key_prefix )
                logs.append(logline)
            except: 
                logline = dict(Requestdate= req_date, currentsc = "Key not found", batch_id = batchid, path = paths['PathId'],key_prefix = key_prefix )
                logs.append(logline)
        count = count + 1

    #Conver the logs into dataFrame        
    dataFrame = pd.DataFrame(data=logs, columns = ['Requestdate','currentsc','batch_id','path','key_prefix'])
  
    # Writing the dataframe to io buffer
    dataFrame.to_csv(csv_buffer)
    
    # Create a s3 resource and create the file in s3
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
    processed_key = 'arch_request' + '/' + 'processed_arch_req' + '/' + archreq_inkey[1]

    
    s3_resource.meta.client.copy(copy_source, 'archrequest', processed_key)