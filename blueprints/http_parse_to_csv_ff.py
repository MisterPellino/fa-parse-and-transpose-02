"""
This function app blueprint is used to parse an excel file to a csv file.
The script is tailored for handeling an xlsm file with a specific format and structure.
It works with exxel table from the following unit of operation (specific processes in the PDP department, fill and finish team):
- STOB, LDIL, STOV, VIA
"""

import azure.functions as func
import logging
import io
# import os
# import re
import json
import pandas as pd
import datetime as datetime


bp = func.Blueprint()


@bp.route(route="http-parse-to-csv-ff")

@bp.blob_input(
    arg_name="excelfile",
    path="{input_path}/{input_file}",
    connection="DATALAKE_STORAGE",
    data_type="binary"
)

@bp.blob_output(
    arg_name="outputblob",
    path="{output_path}/{output_file}",
    connection="DATALAKE_STORAGE_OUTPUT",
    data_type="binary"
)

def http_parse_to_csv_ff(req: func.HttpRequest, excelfile: func.InputStream, outputblob: func.Out[func.InputStream]) -> func.HttpResponse:
    _result = {}
    logging.info("Python HTTP trigger function processed a request.")

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
    
     # check that the input file is the correct excel table

    _units_of_operation = { 'STOB', 'LDIL', 'STOV', 'VIA'}
    logging.info(f"input_file: {input_file}")
    if not any(unit in input_file for unit in _units_of_operation):
        _result = {
            "input_path": input_path,
            "input_file": input_file,
            "output_path": output_path
        }
        _result["error"] = "The input file is not a valid file for this function"
        _result["status_code"] = 400
        return func.HttpResponse(json.dumps(_result), status_code=400, mimetype = "application/json")
    
    ##### read from blob #####
    # Test if the blob is accessible.
    try:
        blob_content = excelfile.read()
        # print(io.BytesIO(blob_content))
        if not blob_content:
            raise ValueError("Blob content is empty")
    except Exception as e:
        _result = {
            "input_file": input_file,
            "input_path": input_path,
            "output_path": output_path,
            "error": f"Failed to read blob: {str(e)}",
            "status_code": 406
        }
        return func.HttpResponse(json.dumps(_result, indent=4), mimetype="application/json", status_code=406)


        # Read the Excel file
    try:
        _blob_io = io.BytesIO(blob_content)
        _df = pd.read_excel(_blob_io, engine='openpyxl')
    except:
        _result["error"] = "INPUT FILE CAN'T BE LOADED"
        _result["status_code"] = 406
        return func.HttpResponse(json.dumps(_result, indent=4), mimetype="application/json", status_code=406)

    # Check if the file is empty
    if _df.empty:
        _result = {
            "input_path": input_path,
            "input_file": input_file,
            "output_path": output_path
        }
        _result["error"] = "EMPTY DATAFRAME"
        _result["status_code"] = 406
        return func.HttpResponse(json.dumps(_result), status_code=406, mimetype = "application/json")

    
    ### Manipulate the data ###
 
    try:
        # remove 0 to 5 rows
        _df = _df.iloc[6:]

        # remove first column
        _df = _df.iloc[:, 1:]

        # reset the header to make it to the first row
        new_header = _df.iloc[0] #grab the first row for the header
        _df = _df[1:] #take the data less the header row
        _df.columns = new_header #set the header row as the _df header

        # remove columns that are not string
        non_string_columns = [col for col in _df.columns if not isinstance(col, str)]
        _df = _df.drop(non_string_columns, axis=1)

        # remove column that starts with ↓
        special_caracter_columns = [
        col for col in _df.columns
            if col.startswith('↑') or col.startswith('↓')
        ]
        
        _df = _df.drop(special_caracter_columns, axis=1)

        # remove rows without ID
        _df = _df.dropna(subset=['Parameter name pivot'])
        _df = _df[~_df['Parameter name pivot'].str.contains('---|Insert')]

        # change column name: change Parameter name pivot' to 'Experiment ID'
        _df = _df.rename(columns={'Parameter name pivot': 'Experiment ID'})

        # convert table to csv
        buffer = io.BytesIO()
        _df.to_csv(buffer, index=True)
        buffer.seek(0)

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

    # get the first 5 rows of the dataframe to check the output in the response upon success.
    _head = _df.head().to_json(orient='records')

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
