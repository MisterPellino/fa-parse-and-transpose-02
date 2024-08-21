import azure.functions as func
import logging
import io
import os
import re
import json
import pandas as pd
import datetime as datetime
from azure.storage.fileshare import ShareFileClient

app = func.FunctionApp()
@app.function_name(name="http-parse-to-csv")
@app.route(route="http-parse-to-csv")

@app.blob_output(
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
    
    # Read the data from the input share files.

    file_client = ShareFileClient.from_connection_string(
       conn_str = os.environ.get("DATALAKE_STORAGE"),
       share_name=input_path,
       file_path=input_file)

    download_stream = file_client.download_file()
    file_content = download_stream.readall()
    ### Manipulate the data ###

    
    try:
        # remove first 6 columns and 2 rows
        _df = _df.iloc[:, 6:]
        _df = _df.iloc[2:, :]

        # delete header
        _df.columns = [None] * len(_df.columns)
        
        # Add new column name for experiment id
        _df.iloc[0, 0] = 'Experiment_ID'

        # reset index
        _df.reset_index(drop=True, inplace=True)

        # transpose the dataframe
        _df_transposed = _df.T

        # delete header again
        _df_transposed.columns = [None] * len(_df_transposed.columns)

        # delete empty columns and rows
        _df_transposed.dropna(axis=1, how='all', inplace=True)
        _df_transposed.dropna(axis=0, how='all', inplace=True)

        # set first row as header
        new_header = _df_transposed.iloc[0]
        _df_transposed = _df_transposed[1:]
        _df_transposed.columns = new_header

        # optinal add index column
        idx = pd.Series(
            range(1, len(_df_transposed) + 1))
        _df_transposed.set_index(idx, inplace=True)
        _df_transposed.index.name = 'id'
        
        # remove columns with special characters
        special_character_columns = [
            col for col in _df_transposed.columns
            if col.startswith('↑') or col.startswith('↓')
        ]
        # _num_removed_cols = len(special_character_columns)
        _df_transposed.drop(special_character_columns, axis=1, inplace=True)

        # remove empty and special rows
        # remove rows with keyword
        keyword = 'Template'
        column = 'Experiment_ID'
        _df_transposed.drop(_df_transposed[_df_transposed[column].str.contains(keyword, na=False)].index, inplace=True)

        # remove empty rows
        _df_transposed.dropna(subset=[column], inplace=True)

        # convert table to csv
        buffer = io.BytesIO()
        _df_transposed.to_csv(buffer, index=True)
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
    _head = _df_transposed.head().to_json(orient='records')

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
