import os
import boto3
from dotenv import load_dotenv
from ftplib import FTP
from ftplib import error_perm
import lzma
import tarfile 
import gzip
import shutil
import subprocess
import threading
import queue
import csv

s3_conn = None 
BUCKET_NAME = 'quadram-bioinfo-allthebacteria'
MAX_WORKERS = 10 

# --- S3 functions ---

def get_or_create_s3_conn(): 
    """
    Get or create an S3 connection using the AWS credentials from the .keys.env file.

    :return: S3 connection object
    """
    global s3_conn
    dotenv_path = '.keys.env' 
    load_dotenv(dotenv_path=dotenv_path)    
    if s3_conn:
        return s3_conn
    # Create a resource using your S3 credentials
    s3 = boto3.resource('s3', endpoint_url='https://s3.climb.ac.uk', aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))
    s3_conn = s3 
    return s3_conn

def upload_file_to_s3(file_path, bucket, prefix='hoard'):
    """
    Upload a file to S3.

    :param file_path: Path to the file to be uploaded
    :param bucket: S3 bucket name
    :param prefix: Prefix for the S3 object key
    """
    file_name = os.path.basename(file_path)
    s3 = get_or_create_s3_conn()
    object = s3.Object(bucket, prefix + '/' + file_name)
    object.put(Body=open(file_path, 'rb'))

def download_file_from_s3(key, bucket, output_path, prefix='hoard'):
    """
    Download a file from S3.

    :param key: S3 object key
    :param bucket: S3 bucket name
    :param output_path: Path to save the downloaded file
    :param prefix: Prefix for the S3 object key
    """
    s3 = get_or_create_s3_conn()
    object = s3.Object(bucket, prefix + '/' + key)
    with open(output_path, 'w') as f:
         f.write(object.get()['Body'].read().decode('utf-8'))

def get_existing_files(directory='hoard/'):
    """
    Get a list of existing files in the specified S3 directory.

    :param directory: S3 directory path
    :return: List of existing file paths
    """
    s3 = get_or_create_s3_conn()
    my_bucket = s3.Bucket(BUCKET_NAME)    
    existing_files = [] 
    for my_bucket_object in my_bucket.objects.filter(Prefix=directory):
        existing_files.append(my_bucket_object.key)
    return existing_files

# --- FTP functions ---

def create_archive_list(species='escherichia_coli', max=75, ftp_dir='pub/databases/AllTheBacteria/Releases/0.2/assembly/'):
    """
    Create a list of archive file paths for a given species.

    :param species: Species name
    :param max: Maximum number of archives to include
    :param ftp_dir: FTP directory path
    :return: List of archive file paths
    """
    all_archives = [] 
    for i in range(max):
        all_archives.append(os.path.join(ftp_dir, species + '__' + str(i).zfill(2) + '.asm.tar.xz'))
    return all_archives

def get_ftp_file(ftp_path, ftp_server='ftp.ebi.ac.uk', local_dir='temp'):
    """
    Download a file from an FTP server.

    :param ftp_path: FTP file path
    :param ftp_server: FTP server address
    :param local_dir: Local directory to save the downloaded file
    :return: Path to the downloaded file
    """
    if not os.path.exists(local_dir):
        os.mkdir(local_dir)
    local_filename = os.path.join(local_dir, os.path.basename(ftp_path))
    ftp = FTP(ftp_server)
    ftp.login()
    if not os.path.exists(local_filename):
        with open(local_filename, 'wb') as file:
            try:
                ftp.retrbinary(f"RETR {ftp_path}", file.write)
            except error_perm:
                print('Cannot fetch ', ftp_path)
        ftp.quit()
        print(f"File downloaded as {local_filename}")
    return local_filename
    

def extract_xz(file_path, output_dir):
    """
    Extract an .xz file.

    :param file_path: Path to the .xz file
    :param output_path: Path to save the extracted file
    :return: Path to the extracted file
    """
    with lzma.open(file_path, 'rb') as xz_file:
        with tarfile.open(fileobj=xz_file) as tar:
            tar.extractall(path=output_dir)
    print(f"Extracted {file_path} to {output_dir}")
    return os.path.join(output_dir, os.path.basename(file_path).split('.')[0]) 

def split_path(filename):
    """
    Split a file path into parts.

    :param filename: File path
    :return: Tuple containing the new path and directory path
    """
    # Remove the extension to get the base name
    base_name = os.path.splitext(os.path.splitext(filename)[0])[0]
    
    # Extract the parts
    part1 = base_name[:7]
    part2 = base_name[:10]
    
    # Construct the new path
    new_path = os.path.join(part1, part2, filename)
    return new_path, os.path.join(part1, part2)


def compress_with_pigz(input_file, output_file):
    """
    Compress a file using pigz.

    :param input_file: Path to the input file
    :param output_file: Path where the compressed file should be saved
    """
    threads = str(max(os.cpu_count() - 1, 1))
    subprocess.run(['pigz', '-p', threads , '-c', input_file], stdout=open(output_file, 'wb'))
    print('Compressed ' , output_file)


class Worker(threading.Thread):
    def __init__(self, q, *args, **kwargs):
        self.q = q
        super().__init__(*args, **kwargs)
        dotenv_path = '.keys.env' 
        load_dotenv(dotenv_path=dotenv_path)    
        # Create a resource using your S3 credentials
        self.s3_conn = boto3.resource('s3', endpoint_url='https://s3.climb.ac.uk', aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))

    def run(self):
        while True:
            try:
                work = self.q.get() 
            except queue.Empty:
                return
            self.upload_file_to_s3(work[0], work[1], prefix=work[2])
            self.q.task_done()

    def upload_file_to_s3(self, file_path, bucket, prefix='hoard'):
        """
        Upload a file to S3.

        :param file_path: Path to the file to be uploaded
        :param bucket: S3 bucket name
        :param prefix: Prefix for the S3 object key
        """
        file_name = os.path.basename(file_path)
        object = self.s3_conn.Object(bucket, prefix + '/' + file_name)
        object.put(Body=open(file_path, 'rb'))
        print('thread uploaded', file_name)

def main():
    print('Reading existing files...')
    existing = get_existing_files() 
    print('Existing files read.')
    q = queue.Queue()
    for _ in range(MAX_WORKERS):
        print('Starting worker', _ )
        Worker(q).start()
    metadata_dir = 'metadata'
    all_accession_path = os.path.join(metadata_dir, 'all_accessions.tsv')
    # Read the all_accession.tsv file as a dictionary
    accession_dict = {}
    with open(all_accession_path, 'r') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            accession_dict[row['biosample']] = row
    for archive in create_archive_list():
        archive_path = get_ftp_file(archive)
        if os.path.exists(archive_path) and os.path.getsize(archive_path) > 100: 
            missing_from_this_tar = [] 
            with tarfile.open(archive_path, 'r:xz') as tar:
                for member in tar.getmembers():
                    hoard_path = 'hoard/' + split_path(os.path.basename(member.name))[0] + '.gz' 
                    if not hoard_path in existing:
                       missing_from_this_tar.append(member.name)
                    else:
                        if accession_dict.get(os.path.basename(member.name.split('.')[0])):
                            accession_dict[os.path.basename(member.name.split('.')[0])]['url'] = 'https://quadram-bioinfo-allthebacteria.s3.climb.ac.uk/' + hoard_path
            if missing_from_this_tar:
                print(f'Missing {len(missing_from_this_tar)} files: ')
                output_fasta_dir = os.path.join('temp', os.path.basename(archive_path).split('.')[0]) 
                if not os.path.exists(output_fasta_dir):
                    output_fasta_dir = extract_xz(archive_path, 'temp/') 
                # Compress all remaining fasta files. 

                file_paths  = {os.path.join(output_fasta_dir, x + '.gz'): os.path.join(output_fasta_dir, x) for x in os.listdir(output_fasta_dir) if x.endswith('.fa') and x in missing_from_this_tar }
                for gz_fasta_path, fasta_path in  file_paths.items():
                    if not os.path.exists(gz_fasta_path):
                        compress_with_pigz(fasta_path, gz_fasta_path)
                    gz_fasta_name = os.path.basename(gz_fasta_path) 
                    s3_full_path, s3_dirs = split_path(gz_fasta_name)      
                    url = 'https://quadram-bioinfo-allthebacteria.s3.climb.ac.uk/' + os.path.join('hoard', s3_full_path) 
                    if accession_dict.get(os.path.basename(gz_fasta_name.split('.')[0])):
                        accession_dict[os.path.basename(gz_fasta_name.split('.')[0])]['url'] = url
                    if not os.path.join('hoard', s3_full_path) in existing:
                        q.put([gz_fasta_path, BUCKET_NAME, os.path.join('hoard', s3_dirs)])
                    else:
                        print('Skipping ', gz_fasta_name )
            else:
                print('All files accounted for. Skipping ', archive_path)
                # delete archive_path file
                os.remove(archive_path)          
        # write accession_dict to new  file              
        outout = os.path.join(metadata_dir, 'all_url_accessions.tsv')    
        with open(outout, 'w') as file:
            fieldnames = ['biosample', 'acc', 'experiment', 'sample_name', 'sample_acc', 'bioproject', 'sra_study', 'url']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in accession_dict.values():
                if row.get('url'):
                    writer.writerow(row)

    q.join()  # blocks until the queue is empty.

main() 
