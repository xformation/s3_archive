# s3_archive
The python lambda function will process a json file and archive the Files provided to Glacier Deep Archive

    The lambda will be triggered on to a specific s3 folders upload event. The upload file is a specific format json that provides the file path and other information.

    The input file will be parsed till all the files archived and once processed it will be moved to processed folder.

    Program also generate a log file which spcify which files have been archived or had issue with the key prefix.
