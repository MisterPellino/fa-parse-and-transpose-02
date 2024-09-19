# fa-parse-and-transpose-02
## HTTP Parse to CSV Function
### Project Scope
This project aims to create a function that is activated by an HTTP request from a Data Factory trigger. The request should contain the necessary fields in the body (see JSON example below). 
The function reads a Blob in XLSM format, if necessary parses and transposes it, and then copies the simplified and parsed table as a CSV file into a storage blob.
The function handles excel file coming from PDP group, these excel files are of 3 main types and need to be handled differently.

### Main Features
- **HTTP Trigger**: Activated by an HTTP request with specific parameters.
- **File Reading**: Reads a Blob in XLSM format from Azure Storage.
- **Data Parsing**: Parses and transposes the data, removing unnecessary columns and rows, and handling special characters.
- **CSV Conversion**: Converts the processed data into a CSV format.
- **Blob Storage**: Writes the CSV file to an Azure Storage Blob.

### Function descriptions
- **http_parse_to_csv_fb**: This function is called only if the original filename contains 'DIL', 'DIS', 'TMIX', 'BBR', 'UFDF'. It reads the Excel file, removes unwanted columns and rows, and transposes the table (the original table is in the wrong orientation for legacy reasons).
- **http_parse_to_csv_ff**: This function is called only if the original filename contains 'STOB', 'LDIL', 'STOV', 'VIA'. It reads the Excel file and removes unwanted columns and rows.
- **http_parse_to_csv_analytica**: This function is called only if the original filename contains 'Analytical'.

### JSON Request Body
The HTTP request should include a JSON body with the following fields:

JSON

{
    "input_path": "<input-share-path>",
    "input_file": "<input-file-name>",
    "output_path": "<output-share-path>",
    "output_file": "<output-file-name>" // optional
}



