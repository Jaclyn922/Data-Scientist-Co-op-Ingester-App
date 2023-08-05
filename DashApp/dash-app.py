
import argparse
import base64
import configparser
import dash
from dash import dcc
from dash import html
import dash.dash_table as dash_table
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

import datetime

#from gen3.auth import Gen3Auth
#from gen3.submission import Gen3Submission
#from gen3.index import Gen3Index
#from gen3.metadata import Gen3Metadata

import hashlib
import json
import logging
import os
import pandas as pd
from pathlib import Path
import requests
import yaml

# Define the generate_file_list_template function

    # Initialize an empty list to store file information in the "new" directory
def get_file_info(ingest_dir):
    new_file_list = []
    for filename in os.listdir(ingest_dir):
        filepath = os.path.join(ingest_dir, filename)
        file_info = {
            "filename": filename,
            "size": "{} bytes".format(os.path.getsize(filepath)),
            "lines": sum(1 for _ in open(filepath)),
            "md5sum": get_md5sum(filepath),
            "owner": get_file_owner(filepath),
            "create_date": get_creation_date(filepath),
        }
        new_file_list.append(file_info)
    return new_file_list

def generate_file_list_template(ingest_dir):
    # Create the table header for both "new" and "done" directories
    new_file_list = get_file_info(ingest_dir)
    table_header = [
        html.Th("Filename"),
        html.Th("Size"),
        html.Th("Lines"),
        html.Th("MD5 Checksum"),
        html.Th("Owner"),
        html.Th("Creation Date"),
    ]

    # Combine the header and rows to create the tables for both "new" and "done" directories
    new_file_list_table = html.Table(
        [html.Tr(table_header)] + (generate_table_rows(new_file_list) if new_file_list else [html.Tr([html.Td("No files found", colSpan=6)])]),
        style={"border": "1px solid black"}
    )

    return new_file_list_table 


def generate_file_info_table(file_info):
    # Create a table to display the file info
    table_header = [html.Th("Column Name"), html.Th("Value")]
    table_rows = []

    for key, value in file_info.items():
        row = html.Tr([html.Td(key), html.Td(value)])
        table_rows.append(row)

    file_info_table = html.Table([html.Tr(table_header)] + table_rows, style={"border": "1px solid black"})
    return file_info_table

def generate_table_rows(file_list):
    # Generate the rows for the table
    rows = []
    for file_info in file_list:
        filename = file_info.get("filename", "Unknown")
        size = file_info.get("size", "Unknown")
        lines = file_info.get("lines", "Unknown")
        md5sum = file_info.get("md5sum", "Unknown")
        owner = file_info.get("owner", "Unknown")
        create_date = file_info.get("create_date", "Unknown")

        row = html.Tr([
            html.Td(filename),
            html.Td(size),
            html.Td(lines),
            html.Td(md5sum),
            html.Td(owner),
            html.Td(create_date),
        ])
        rows.append(row)

        # # Append the file info table as a hidden row
        # hidden_info_row = html.Tr([
        #     html.Td(generate_file_info_table(file_info), colSpan=6, style={"display": "none"}, id=f"info-table-{filename}")
        # ])
        # rows.append(hidden_info_row)

    return rows


# Define the function to move loaded files to the "done" directory
def move_loaded_files(loaded_files):
    for file_info in loaded_files:
        filepath = os.path.join(ingest_dir, file_info["filename"])
        os.rename(filepath, os.path.join(done_dir, file_info["filename"]))

# Define the md5_checksum 
def get_md5sum(filepath):
    with open(filepath, "rb") as file:
        data = file.read()
        md5_checksum = hashlib.md5(data).hexdigest()
    return md5_checksum

def get_file_owner(filepath):
    """ Get the owner of the file"""
    # https://stackoverflow.com/questions/1830618/how-to-find-the-owner-of-a-file-or-directory-in-python
    path = Path(filepath)
    owner = path.owner()
    return owner

# Define the creation date of a file
def get_creation_date(filepath):
    creation_time = os.path.getctime(filepath)
    formatted_date = datetime.datetime.fromtimestamp(creation_time).strftime('%Y-%m-%d %H:%M:%S')
    return formatted_date

# Define functIon to create a new file list
# def generate_new_file_list():
#     new_files = [
#        {"filename": "new_file_1.txt", "size": "10KB", "owner": "John Doe", "date": "2023-07-29"},
#        {"filename": "new_file_2.txt", "size": "15KB", "owner": "Jane Smith", "date": "2023-07-30"},
#        # Add more files as needed
#    ]

#    # use Gen3Index as follows:
#    index = Gen3Index("https://chandemo5.bwh.harvard.edu/", auth_provider=auth)
#    if not index.is_healthy():
#        print(f"Uh oh! The indexing service is not healthy in the commons https://chandemo5.bwh.harvard.edu/")
#        exit()

#   print("Some file stats:")
#    print(index.get_stats())

#    print("Example GUID record:")
#    print(index.get(guid="afea506a-62d0-4e8e-9388-19d3c5ac52be"))
#    return new_files

if __name__ == '__main__':

    LOG = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(description=f"Gen3 Ingester App", usage=f"python DashApp/dash-app.py --configfile=conf/chandemo5.yaml")
    parser.add_argument('--configfile', type=str, help="Configuration file containing run settings")
    args = parser.parse_args()

    with open(args.configfile) as c:
        config = yaml.load(c)
    LOG.debug(f'{config=}')
    base_ingest_dir = Path(config['base_ingest_dir'])
    ingest_dir = base_ingest_dir/'new'
    done_dir = base_ingest_dir/'done'

    gen3_base_url = config['gen3_base_url']

    app = dash.Dash(__name__, suppress_callback_exceptions=True)
    
    # Example placeholder for db_col_names. What is the name in the database?
    # db_col_names = ["Column1(can change later)", "Column2", "Column3"]

    # I have the code to generate new file at top, but for some reason it not works. so I add this file.
    # new_file_list = generate_new_file_list()

    # Create a div to display the file info table
    file_info_table_div = html.Div(id="file-info-table-container")

    app.layout = html.Div([
        dcc.Location(id='url', refresh=False),
        html.H1("CDNM Gen3 Ingester"),

        html.Div([
            # File list table
            html.Div([
                html.H3("Files in Ingest Directory"),
                dash_table.DataTable(
                    id="file-list-table",
                    columns=[
                        {"name": "Filename", "id": "filename"},
                        {"name": "Size", "id": "size"},
                        {"name": "Lines", "id": "lines"},
                        {"name": "MD5 Checksum", "id": "md5sum"},
                        {"name": "Owner", "id": "owner"},
                        {"name": "Creation Date", "id": "create_date"},
                    ],
                    data=get_file_info(ingest_dir),  # The actual data will be populated dynamically
                    style_table={"border": "1px solid black"},
                ),
            ], id="file-list-container"),

            # File selection dropdown
            html.H3("Select file to upload"),
            dcc.Dropdown(
                id='file-dropdown',
                options=[{'label': filename, 'value': str(ingest_dir/filename)} for filename in os.listdir(ingest_dir)],
                placeholder="Select a file...",
                style={'margin': '10px'}
            ),
            
            # Load Buttons
            html.Button("Load All", id="load-all-button", n_clicks=0, style={"margin": "10px"}),
            html.Button("Load File", id="load-file-button", n_clicks=0, style={"margin": "10px"}),
            

            # Result box to show loaded file's information
            html.Div([dcc.Textarea(
                id='textarea-log-output',
                value='Textarea content initialized\nwith multiple lines of text',
                style={'width': '100%', 'height': 300},
                ),]),
        ]),

        # Define the dcc.Store components to store the file lists
        # dcc.Store(id="new-file-list-store", data=new_file_list),
        # dcc.Store(id="done-file-list-store", data=done_file_list if 'done_file_list' in globals() else [])
    ])

    
    #@app.callback(
    #    [Output("textarea-log-output-box", "contents"),
    #        Output("file-list-table", "data")],
    #    [Input("upload-data", "contents"),
    #        Input("load-all-button", "n_clicks"),
    #        Input("load-file-button", "n_clicks")],
    #    [State('upload-data', 'filename'),
    #        State("ingest_dir", "children"),
    #        State("file-dropdown", "value"),
    #        State("file-list-table", "data")]
    #)
    @app.callback(
        Output("textarea-log-output", "value"),
        Input("load-file-button", "n_clicks"),
        State('file-dropdown', 'value')
        )
    def handle_data_and_file_loading(n_clicks, selected_filepath):
        LOG.debug(f'handle_data_and_file_loading({selected_filepath=})')
        if not dash.callback_context.triggered:
            raise PreventUpdate

        trigger_id = dash.callback_context.triggered[0]["prop_id"]
        LOG.debug(f'{trigger_id=})')

        file_list_data = get_file_info(ingest_dir)

        if not selected_filepath:
            return (
                dash.no_update,
                dash.no_update,
                "Please select a file to upload.",
                file_list_data
            )

        # I need to add stuffs here to show how load the selected file into Gen3

        # Move the loaded file from "new" to "done" directory (simulating the move)
        # file_info = next(item for item in file_list_data if item["filename"] == selected_filepath)
        # file_info["status"] = "Loaded"  # Add a status field to indicate loaded status

        # Return the updated file list and the loaded file's information
        #auth = Gen3Auth(refresh_file="credentials.json")
        #mds = Gen3Metadata(auth_provider=auth)

        # Save the copied credentials.json from the website and paste the api_key and key_id into a variable "key":
        with open('/udd/rejpz/.gen3/credentials.json') as fh:
            key = json.load(fh)
        print(f'{key=}')

        # Pass the API key to the Gen3 API using "requests.post" to receive the access token:
        token_rq = requests.post(f'{gen3_base_url}/user/credentials/cdis/access_token', json=key)
        if token_rq.status_code != 200:
            return token_rq.text
        token = token_rq.json()
        print(f'{token=}')

        headers = {'Authorization': 'bearer '+ token['access_token']}

        # Data Upload via API Endpoint Request: Needs to be Chunked (Otherwise you get a 413 Error)
        print(f'reading {selected_filepath} ...')
        program = 'g0'
        project = 'p0'
        chunksize = 1000
        for chunk in pd.read_csv(selected_filepath, chunksize=chunksize):
            # chunk is a DataFrame. To "process" the rows in the chunk:
            raw = chunk.to_csv().encode('utf-8')
            print(f'putting ...')
            if 'program' in selected_filepath:
                submission_api_path = '_root'
            else:
                submission_api_path = f'api/v0/submission/{program}/{project}'
            u = requests.put(f'{gen3_base_url}/{submission_api_path}', data=raw, headers=headers)
            print(f'{u.status_code=}')
            print(f'{u.text=}') # should display the API response
        return 'DONE!'

    # open on http://172.27.104.17:8050/
    app.run_server("0.0.0.0", debug=True)