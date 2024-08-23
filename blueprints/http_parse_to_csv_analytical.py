"""
This function app blueürint is used to parse an excel file to a csv file.
The script is tailored for handeling an xlsm file with a specific format and structure.
It works with exxel table from the Analytical excel tables
"""

import azure.functions as func
import logging
import io
import os
import re
import json
import pandas as pd
import datetime as datetime
import warnings
from azure.storage.fileshare import ShareFileClient
from io import BytesIO

bp = func.Blueprint()

@bp.function_name(name="http-parse-to-csv-analytical")
@bp.route(route="http-parse-to-csv-analytical")

@bp.blob_output(
    arg_name="outputblob",
    path="{output_path}/{output_file}",
    connection="DATALAKE_STORAGE_OUTPUT",
    data_type="binary"
)

def main(req: func.HttpRequest,  outputblob: func.Out[func.InputStream]) -> func.HttpResponse:
    _result = {}
    
    try:
        req_body = req.get_json()
    except ValueError:
        pass
    else:
        input_path = req_body.get("input_path")
        input_file = req_body.get("input_file")
        output_path = req_body.get("output_path")
        output_file = req_body.get("output_file")
        
    
    if None in {input_path, input_file, output_path}:
        _result = {
            "input_file": input_file,
            "input_path": input_path,
            "output_path": output_path
        }
        _result["error"] = "MANDATORY PARAMETERS ARE MISSING"
        _result["status_code"] = 400
        return func.HttpResponse(json.dumps(_result, indent=4), mimetype="application/json", status_code=400)
    
    _units_of_operation = {'Analytical'}

    if not any(unit in input_file for unit in _units_of_operation):
        _result = {
            "input_path": input_path,
            "input_file": input_file,
            "output_path": output_path
        }
        _result["error"] = "The input file is not a valid file for this function"
        _result["status_code"] = 400
        return func.HttpResponse(json.dumps(_result), status_code=400, mimetype = "application/json")
    
    # Read the data from the input share files.

    file_client = ShareFileClient.from_connection_string(
       conn_str = os.environ.get("DATALAKE_STORAGE"),
       share_name=input_path,
       file_path=input_file)

    download_stream = file_client.download_file()
    file_content = download_stream.readall()

    try:
        # Suppress the specific UserWarning
        warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
         # Wrap the byte string in a BytesIO object
        file_content_io = BytesIO(file_content)
        _df = pd.read_excel(file_content_io, engine='openpyxl', header=None)
    except Exception as e:
        _result = {
            "input_path": input_path,
            "input_file": input_file,
            "output_path": output_path
        }
        _result["error"] = "Error reading the file"
        _result["status_code"] = 500
        return func.HttpResponse(json.dumps(_result), status_code=500, mimetype = "application/json")
    
    ### Manipulate the data ###

    try:
        logging.info(f"Initial DataFrame shape: {_df.shape}")

        # remove the first four rows
        _df = _df.iloc[4:]
        logging.info(f"Shape after removing first four rows: {_df.shape}")

        # make the first row the header
        _df.columns = _df.iloc[0]
        logging.info(f"Columns set to: {_df.columns.tolist()}")

        # Drop the first row now that it's set as the header
        _df = _df[1:]
        logging.info(f"Shape after dropping the first row: {_df.shape}")

        # Reset the index
        _df.reset_index(drop=True, inplace=True)
        logging.info("Index reset")

        logging.info(f"Columns set to: {_df.columns.tolist()}")

        # correct error in datetime colums
        _df['Spalte_Compiling_Timestamp'] = pd.to_datetime(_df['Spalte_Compiling_Timestamp'], errors='coerce')
        logging.info("Datetime conversion applied")

         # convert table to csv
        buffer = io.BytesIO()
        _df.to_csv(buffer, index=True)
        buffer.seek(0)
        logging.info("DataFrame converted to CSV")

    except Exception as e:
        logging.error(e)
        _result = {
            "input_path": input_path,
            "input_file": input_file,
            "output_path": output_path
        }
        _result["error"] = "Error parsing the data frame"
        _result["status_code"] = 500
        return func.HttpResponse(json.dumps(_result), status_code=500, mimetype = "application/json")

    ### End data manipulation ###    

    # get the first 5 rows of the dataframe to return in the response

    _head = _df.head().to_json(orient="records")
    # Write the data to the output blob.
    try:
        outputblob.set(buffer.getvalue())
    except Exception as e:
        _result = {
            "input_file": input_file,
            "input_path": input_path,
            "output_path": output_path,
            "output_file": output_file
        }
        _result["error"] = f"Failed to write to output blob: {str(e)}"
        _result["status_code"] = 500
        return func.HttpResponse(json.dumps(_result, indent=4), mimetype="application/json", status_code=500)
                                 
    # Return a success message and print the input parameters
    _result = {
        "input_file": input_file,
        "input_path": input_path,
        "output_path": output_path
    }
    _result["status_code"] = 200
    _result["message"] = "SUCCESS"
    _result["first_5_rows"] = _head
    return func.HttpResponse(json.dumps(_result, indent=4), mimetype="application/json", status_code=200)