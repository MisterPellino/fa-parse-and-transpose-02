import azure.functions as func
import logging
import json
import pandas as pd
import datetime as datetime
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http-parse-to-csv")
def http_parse_to_csv(req: func.HttpRequest, excel_table_file: func.InputStream) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # get query parameters and catch errors
    try:
        req_body = req.get_json()
    except ValueError:
        pass
    else:
        input_path = req_body.get("input_path")
        input_file = req_body.get("input_file")
        output_path = req_body.get("output_path")

    if None in {input_path, input_file, output_path}:
        _result = {
            "input_path": input_path,
            "input_file": input_file,
            "output_path": output_path
        }
        _result["error"] = "Mandatory parameters are missing"
        _result["status_code"] = 400 
        return func.HttpResponse(json.dumps(_result), status_code=400, mimetype = "application/json" )
    
    # check if the input file is a recognized unit of operation

    _units_of_operation = ['DIS', 'DIL', 'TMIX', 'UFDF', 'BBR', 'STOB', 'VIA', 'STOV' , 'LDIL', 'STVT']

    if not any(unit in input_file for unit in _units_of_operation):
        _result = {
            "input_path": input_path,
            "input_file": input_file,
            "output_path": output_path
        }
        _result["error"] = "Invalid input file: not a recognized unit of operation"
        _result["status_code"] = 400 
        return func.HttpResponse(json.dumps(_result), status_code=400, mimetype = "application/json")
                                 
    # set metadata
    
    _unit_of_operation = "Unknown"
    for unit in _units_of_operation:
        if unit in input_file:
            _unit_of_operation = unit
            break

    _timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    _output_file = f"{input_file}_processed_{_timestamp}.csv"

    # if file represent a unit of operation, process it

    # read the excel file
    try:
        df = excel_table_file.read()
    except Exception as e:
        logging.error(e)
        _result = {
            "input_path": input_path,
            "input_file": input_file,
            "output_path": output_path
        }
        _result["error"] = "Error reading the excel file"
        _result["status_code"] = 500
        return func.HttpResponse(json.dumps(_result), status_code=500, mimetype = "application/json")
    
    ## parse data frame and remove unnecessary colums and row, the transpose the data frame ##

    try:
        # remove first 6 columns and 2 rows
        df = df.iloc[:, 6:]
        df = df.iloc[2:, :]

        # delete header
        df.columns = [None] * len(df.columns)

        # Add new column name for experiment id
        df.iloc[0, 0] = 'Experiment_ID'

        # reset index
        df.reset_index(drop=True, inplace=True)

        # transpose the dataframe
        df_transposed = df.T

        # delete header again
        df_transposed.columns = [None] * len(df_transposed.columns)

        # delete empty columns and rows
        df_transposed = df_transposed.dropna(axis=1, how='all', inplace=True)
        df_transposed = df_transposed.dropna(axis=0, how='all', inplace=True)

        # set first row as header
        new_header = df_transposed.iloc[0]
        df_transposed = df_transposed[1:]
        df_transposed.columns = new_header

        # optinal add index column
        idx = pd.Series(
            range(1, len(df_transposed) + 1))
        df_transposed.set_index(idx, inplace=True)
        df_transposed.index.name = 'id'
        
        # remove columns with special characters
        special_character_columns = [
            col for col in df.columns
            if col.startswith('↑') or col.startswith('↓')
        ]
        num_removed_cols = len(special_character_columns)
        df_transposed.drop(special_character_columns, axis=1, inplace=True)

        # remove empty and special rows
        # remove rows with keyword
        keyword = 'Template'
        column = 'Experiment_ID'
        df_transposed.drop(df_transposed[df_transposed[column].str.contains(keyword, na=False)].index, inplace=True)

        # remove empty rows
        df_transposed.dropna(subset=[column], inplace=True)

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
    
    # save the processed data frame to a csv file
    output_file = input_file.split('.')[0] + '_processed_' + _timestamp + '.csv'
    blob_csv = BlobServiceClient.get_blob_client(container=output_path, blob=output_file)
    blob_csv.upload_blob(df.to_csv(index=False))

    logging.info(f"Processed data saved as csv to {output_file}")

    _result["output_file"] = output_file
    _result["status_code"] = 200

    return func.HttpResponse(json.dumps(_result, indent=4), mimetype="application/json", status_code=200)