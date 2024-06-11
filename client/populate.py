from google.cloud import bigquery
import csv
import os
import datetime

def run_accessions_query(all_accessions_path):
    client = bigquery.Client()
    # Run the code to populate the file
    # Perform a query.
    QUERY = (
        "SELECT acc, experiment, sample_name, sample_acc, biosample, bioproject, sra_study FROM `nih-sra-datastore.sra.metadata` WHERE (organism LIKE 'Escherichia%' OR organism LIKE 'Shigella%') AND assay_type = 'WGS' AND platform = 'ILLUMINA'")
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    # Open the output file in write mode
    with open(all_accessions_path, 'w', newline='') as file:
        # Create a CSV writer object
        writer = csv.writer(file, delimiter='\t')
        # Write the header row
        writer.writerow(['acc', 'experiment', 'sample_name', 'sample_acc', 'biosample', 'bioproject', 'sra_study'])
        # Iterate over the rows and write each row as a tab-separated values
        for row in rows:
            writer.writerow([row.acc, row.experiment, row.sample_name, row.sample_acc, row.biosample, row.bioproject, row.sra_study])
    # Print a message indicating the file has been created
    print(f"Output file '{all_accessions_path}' has been created.")

def update_accessions_file(all_accessions_path, hours =8):
    # Check if the file exists
    if os.path.exists(all_accessions_path):
        # Get the modification time of the file
        modification_time = os.path.getmtime(all_accessions_path)
        # Convert the modification time to a datetime object
        modification_datetime = datetime.datetime.fromtimestamp(modification_time)
        # Calculate the time difference between now and the modification time
        time_difference = datetime.datetime.now() - modification_datetime
        # Check if the time difference is greater than 8 hours
        if time_difference.total_seconds() > hours * 60 * 60:
            # Update the file
            run_accessions_query(all_accessions_path)
    else:
        # Update the file
        run_accessions_query(all_accessions_path)    
    

def main():
    # Specify the path of the all_accessions file
    all_accessions_path = 'metadata/all_accessions.tsv'
    update_accessions_file(all_accessions_path, hours =8)




main()