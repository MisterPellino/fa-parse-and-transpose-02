# fa-parse-and-transpose-02
## HTTP Parse to CSV Function
### Project Scope
This project aims to create a function that is activated by an HTTP request from a Data Factory trigger. The request should contain the necessary fields in the body (see JSON example below). 
The function reads a ShareFile in XLSM format, parses and transposes it, and then copies the simplified and parsed table as a CSV file into a storage blob.

### Main Features
HTTP Trigger: Activated by an HTTP request with specific parameters.
File Reading: Reads a ShareFile in XLSM format from Azure Storage.
Data Parsing: Parses and transposes the data, removing unnecessary columns and rows, and handling special characters.
CSV Conversion: Converts the processed data into a CSV format.
Blob Storage: Writes the CSV file to an Azure Storage Blob.


### JSON Request Body
The HTTP request should include a JSON body with the following fields:

JSON

{
    "input_path": "<input-share-path>",
    "input_file": "<input-file-name>",
    "output_path": "<output-share-path>",
    "output_file": "<output-file-name>"
}



