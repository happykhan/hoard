import pandas as pd 

# Define the path to the TSV file
file_path = 'metadata/ena_metadata.tsv'

# Read the TSV file
df = pd.read_csv(file_path, sep='\t')

# Display the DataFrame
print(df)
