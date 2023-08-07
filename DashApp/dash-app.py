
import argparse
import dash
from dash import dcc
from dash import html
import dash.dash_table as dash_table
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

import datetime

from gen3.tools.metadata.ingest_manifest import async_ingest_metadata_manifest
from gen3.index import Gen3Index
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission


import hashlib
import json
import logging
import os
import pandas as pd
from pathlib import Path
import requests
import yaml
import structlog

# def configure_logging():
#     structlog.configure(
#         processors=[
#             structlog.stdlib.add_logger_name,
#             structlog.stdlib.add_log_level,
#             structlog.stdlib.PositionalArgumentsFormatter(),
#             structlog.processors.TimeStamper(fmt="iso"),
#             structlog.processors.StackInfoRenderer(),
#             structlog.processors.format_exc_info,
#             structlog.processors.UnicodeDecoder(),
#             structlog.processors.JSONRenderer(),
#         ],
#         context_class=structlog.threadlocal.wrap_dict(dict),
#         logger_factory=structlog.stdlib.LoggerFactory(),
#         wrapper_class=structlog.stdlib.BoundLogger,
#         cache_logger_on_first_use=True,
#     )

def get_file_info(ingest_dir):
    new_file_list = []
    for filename in sorted(os.listdir(ingest_dir)):
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

if __name__ == '__main__':
    # configure_logging()
    # logger = structlog.get_logger()

    # parser = argparse.ArgumentParser(description=f"Gen3 Ingester App", usage=f"python DashApp/dash-app.py --configfile=conf/chandemo5.yaml")

    # FIXME: improve logging (structlog? loki?)
    LOG = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(description=f"Gen3 Ingester App", usage=f"python DashApp/dash-app.py --configfile=conf/chandemo5.yaml")
    parser.add_argument('--configfile', type=str, help="Configuration file containing run settings")
    args = parser.parse_args()

    # Get settings from the input configuration file
    with open(args.configfile) as c:
        config = yaml.load(c)
    LOG.debug(f'{config=}')
    base_ingest_dir = Path(config['base_ingest_dir'])
    ingest_dir = base_ingest_dir/'new'
    done_dir = base_ingest_dir/'done'

    gen3_base_url = config['gen3_base_url']

    app = dash.Dash(__name__, suppress_callback_exceptions=True)
    
    app.layout = html.Div([
        dcc.Location(id='url', refresh=False),
        html.H1("CDNM Gen3 Ingester"),

        html.Div([
            # File selection dropdown
            html.H3(f"Select file to upload ({ingest_dir=})"),
            dcc.Dropdown(
                id='file-dropdown',
                options=[{'label': str(ingest_dir/filename), 'value': str(ingest_dir/filename)} for filename in sorted(os.listdir(ingest_dir))],
                placeholder="Select a file...",
                style={'margin': '10px'}
            ),
            
            # Load Buttons
            html.Button("Load All", id="load-all-button", n_clicks=0, style={"margin": "10px"}),
            html.Button("Load File", id="load-file-button", n_clicks=0, style={"margin": "10px"}),
            

            # Result box to show loaded file's information
            html.Div([dcc.Input(
                id='status-code-display',
                value='',
                style={'width': '10'},
                ),]),

            # Result box to show loaded file's response from the server
            html.Div([dcc.Textarea(
                id='textarea-log-output',
                value='Textarea content initialized\nwith multiple lines of text',
                style={'width': '100%', 'height': 300},
                ),]),

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
        [
            Output("textarea-log-output", "value"),
            Output("status-code-display", "value"),
        ],
        Input("load-file-button", "n_clicks"),
        State('file-dropdown', 'value')
        )
    
    # @app.callback(
    #     [
    #         Output("textarea-log-output", "value"),
    #         Output("status-code-display", "value"),
    #     ],
    #     Input("load-file-button", "n_clicks"),
    #     State('file-dropdown', 'value')
    # )

    def handle_data_and_file_loading(n_clicks, selected_filepath):
        LOG.debug(f'handle_data_and_file_loading({selected_filepath=})')
        if not dash.callback_context.triggered:
            raise PreventUpdate

        #logger.debug(f'handle_data_and_file_loading({selected_filepath=})')

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

        auth = Gen3Auth()
        #mds = Gen3Metadata(auth_provider=auth)

        # Save the copied credentials.json from the website and paste the api_key and key_id into a variable "key":
        # python should be able to get user's home directory (maybe "import getuser"?)
        with open('/udd/rejpz/.gen3/credentials.json') as fh:
            key = json.load(fh)
        print(f'{key=}')

        # Pass the API key to the Gen3 API using "requests.post" to receive the access token:
        token_rq = requests.post(f'{gen3_base_url}/user/credentials/cdis/access_token', json=key)
        if token_rq.status_code != 200:
            return (token_rq.text, token_rq.status_code)
        token = token_rq.json()
        print(f'{token=}')

        headers = {'Authorization': 'bearer '+ token['access_token']}

        # Data Upload via API Endpoint Request: Needs to be Chunked (Otherwise you get a 413 Error)
        print(f'reading {selected_filepath} ...')
        program = 'g0'
        project = 'p0'
        chunksize = 1000
        """
        for chunk in pd.read_csv(selected_filepath, chunksize=chunksize):
            # chunk is a DataFrame. To "process" the rows in the chunk:
            raw = chunk.to_csv().encode('utf-8')
            if 'program' in selected_filepath:
                print(f'creating program')
                sub = Gen3Submission(auth)
                js = {'type':'program', 'name':'g0', 'dbgap_accession_number':'g00000000'}
                sub.create_program(js)
                #submission_api_path = '_root'
            elif 'project' in selected_filepath:
                print(f'creating project')
                sub = Gen3Submission(auth)
                js = {'type': 'project', 'name': 'p0', 'code': 'p0', 'dbgap_accession_number':'p00000000'}
                sub.create_project('g0', js)
                #submission_api_path = '_root'
            else:
                #print(f'putting ...')
                #submission_api_path = f'api/v0/submission/{program}/{project}'
                #u = requests.put(f'{gen3_base_url}/{submission_api_path}', data=raw, headers=headers)
                #print(f'{u.status_code=}')
                #print(f'{u.text=}') # should display the API response
                gen3.tools.metadata.ingest_manifest.async_ingest_metadata_manifest(gen3_base_url, selected_filepath, 'cdnm', auth=auth,
                                                                                max_concurrent_requests=24,
                                                                                manifest_file_delimiter=None,
                                                                                output_filename='ingest-metadata-manifest-errors-1691080449.068882.log')
        """
        if 'program' in selected_filepath:
            print(f'creating program')
            sub = Gen3Submission(auth)
            js = {'type':'program', 'name':'g0', 'dbgap_accession_number':'g00000000'}
            sub.create_program(js)
            #submission_api_path = '_root'
        elif 'project' in selected_filepath:
            print(f'creating project')
            sub = Gen3Submission(auth)
            js = {'type': 'project', 'name': 'p0', 'code': 'p0', 'dbgap_accession_number':'p00000000'}
            sub.create_project('g0', js)
            #submission_api_path = '_root'
        else:
            #print(f'putting ...')
            #submission_api_path = f'api/v0/submission/{program}/{project}'
            #u = requests.put(f'{gen3_base_url}/{submission_api_path}', data=raw, headers=headers)
            #print(f'{u.status_code=}')
            #print(f'{u.text=}') # should display the API response
            print(f'calling gen3.tools.metadata.ingest_manifest.async_ingest_metadata_manifest')
            f = async_ingest_metadata_manifest(gen3_base_url, selected_filepath, 'cdnm', auth=auth,
                                                                            max_concurrent_requests=24,
                                                                            manifest_file_delimiter=None,
                                                                            output_filename='ingest-metadata-manifest-errors-1691080449.068882.log')
            import asyncio
            asyncio.run(f)

        return (u.text, u.status_code) ## this messaging to the user should be chunked as well

    # open on http://172.27.104.17:8050/
    app.run_server("0.0.0.0", debug=True)