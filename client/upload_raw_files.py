import os
from ftplib import FTP, error_perm
import boto3
from dotenv import load_dotenv


VERSION = '0.2'

def get_existing_files(s3_bucket_name, directory='hoard/', no_dir=False):
    """
    Get a list of existing files in the specified S3 directory.

    :param directory: S3 directory path
    :return: List of existing file paths
    """
    s3 = boto3.resource('s3', endpoint_url='https://s3.climb.ac.uk', aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))
    my_bucket = s3.Bucket(s3_bucket_name)    
    existing_files = [] 
    for my_bucket_object in my_bucket.objects.filter(Prefix=directory):
        if no_dir:
            existing_files.append(my_bucket_object.key.split(directory)[1][1:])
        else:
            existing_files.append(my_bucket_object.key)
    return existing_files

def upload_files_ftp_to_s3(ftp_url, s3_bucket_name):
    # Connect to the FTP server
    ftp = FTP(ftp_url)
    ftp.login()
    ftp_path = f'/pub/databases/AllTheBacteria/Releases/{VERSION}/'
    ftp.cwd(ftp_path)  # Change directory to the desired location
    dir_list = ['.', 'assembly', 'indexes/phylign', 'indexes/sketchlib', 'metadata']
    dir_list = [f'{ftp_path}{dir_name}' for dir_name in dir_list]
    # Upload files to S3
    dotenv_path = '.keys.env' 
    load_dotenv(dotenv_path=dotenv_path)     
    s3 = boto3.resource('s3', endpoint_url='https://s3.climb.ac.uk', aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))
    existing_files = get_existing_files(s3_bucket_name, directory='0.2', no_dir=True)
    for dir_name in dir_list:
        file_list = []
        ftp.retrlines(f'LIST {dir_name}', file_list.append)

        for file_rows in file_list:
            if dir_name.endswith('.'):
                file_name = file_rows.split()[-1]
                remote_path = os.path.join(dir_name[:-1], file_name)
                final_path = os.path.basename(remote_path.split(ftp_path)[-1])
                local_file_path = os.path.join('tmp', file_name)
            else:
                file_name = file_rows.split()[-1]
                remote_path = dir_name + '/' + file_name
                final_path = remote_path.split(ftp_path)[-1]
                local_file_path = os.path.join('tmp', file_name)

            
            if file_rows.startswith('-') and final_path not in existing_files:
                print(f'Uploading:\n    {final_path}\nfrom:\n    {remote_path}\nvia:\n     {local_file_path}')
                with open(local_file_path, 'wb') as file:
                    ftp.retrbinary('RETR ' + remote_path, file.write)
                s3.meta.client.upload_file(local_file_path, s3_bucket_name, f'{VERSION}/{final_path}')
                os.remove(local_file_path)
            else:
                print(f'Skipping {final_path} as it already exists in S3.')

    # Disconnect from the FTP server
    ftp.quit()

# Usage
ftp_url = 'ftp.ebi.ac.uk'
s3_bucket_name = 'quadram-bioinfo-allthebacteria'
upload_files_ftp_to_s3(ftp_url, s3_bucket_name)