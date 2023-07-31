# import configparser
# import psycopg2


# def config(filename='/udd/rexin/oracle_loading/DashApp/config.ini', section='database'):
#     parser = configparser.ConfigParser()
#     parser.read(filename)
#     if parser.has_section(section):
#         params = parser.items(section)
#     else:
#         raise Exception(f'Section {section} not found in the {filename} file')
#     return dict(params)

# def connect():
#     conn = None
#     try:
#         params = config()

#         print('Connecting to db')
#         conn = psycopg2.connect(**params)
#         cur = conn.cursor()
#         print('db version:')
#         cur.execute("""
#                 SELECT table_name
#                 FROM information_schema.tables
#                 WHERE table_schema = 'public'

#                 """
#         )
#         tables = cur.fetchall()
#         for table in tables:
#             print(f"Table: {table[0]}")
#             cur.execute(f"SELECT * FROM {table[0]}")
#             rows = cur.fetchall()
#             for row in rows:
#                 print(row)
            

#         cur.close()
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#     finally:
#         if conn is not None:
#             conn.close()
#             print('Database connection closed.')


# if __name__ == '__main__':
#     connect()



import dash
import os
from dash import dcc
from dash import html
import dash.dash_table as dash_table
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dash import dcc
import plotly.express as px
import pandas as pd
import io
import configparser
import base64
import utils
import hashlib
import datetime

# Define the generate_file_list_template function
def generate_file_list_template(new_directory, done_directory, new_file_list=None, done_file_list=None):
    if new_file_list is None:
        new_file_list = []
    if done_file_list is None:
        done_file_list = []

    # Create a table to display the file list for "new" directory
    # Dummy data for new_file_list and done_file_list
    if not new_file_list:
        new_file_list = [
            {"filename": "file1.txt", "size": "10 KB", "lines": 100, "md5_checksum": "abc123", "owner": "user1", "creation_date": "2023-07-29"},
            {"filename": "file2.txt", "size": "5 KB", "lines": 50, "md5_checksum": "def456", "owner": "user2", "creation_date": "2023-07-28"},
        ]

    if not done_file_list:
        done_file_list = [
            {"filename": "file3.txt", "size": "8 KB", "lines": 80, "md5_checksum": "xyz789", "owner": "user3", "creation_date": "2023-07-27"},
            # Add more files as needed
        ]

    # Initialize an empty list to store file information in the "new" directory
    for filename in os.listdir(new_directory):
        filepath = os.path.join(new_directory, filename)
        file_info = {
            "filename": filename,
            "size": "{} bytes".format(os.path.getsize(filepath)),
            "lines": sum(1 for _ in open(filepath)),
            "md5sum": get_md5sum(filepath),
            "owner": get_file_owner(filepath),
            "create_date": get_creation_date(filepath),
        }
        new_file_list.append(file_info)

    # Create a table to display the file list for "done" directory
    # Create an empty list to store info in "done" directory
    done_file_list = []  
    for filename in os.listdir(done_directory):
        filepath = os.path.join(done_directory, filename)
        file_info = {
            "filename": filename,
            "size": "{} bytes".format(os.path.getsize(filepath)),
            "lines": sum(1 for _ in open(filepath)),
            "md5sum": get_md5sum(filepath),
            "owner": get_file_owner(filepath),
            "create_date": get_creation_date(filepath),
        }
        done_file_list.append(file_info)

    # Create the table header for both "new" and "done" directories
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

    done_file_list_table = html.Table(
        [html.Tr(table_header)] + (generate_table_rows(done_file_list) if done_file_list else [html.Tr([html.Td("No files found", colSpan=6)])]),
        style={"border": "1px solid black"}

    )
    # Do we need to return table seperately? Can change later
    return new_file_list_table, done_file_list_table 


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

        # Append a row with a button to show the file info table
        info_row = html.Tr([
            html.Td(html.Button(f"Show Info for {filename}", id=f"info-button-{filename}", n_clicks=0, style={"margin": "5px"}), colSpan=6)
        ])
        rows.append(info_row)

        # # Append the file info table as a hidden row
        # hidden_info_row = html.Tr([
        #     html.Td(generate_file_info_table(file_info), colSpan=6, style={"display": "none"}, id=f"info-table-{filename}")
        # ])
        # rows.append(hidden_info_row)

    return rows



# Define the md5_checksum 
def get_md5sum(filepath):
    with open(filepath, "rb") as file:
        data = file.read()
        md5_checksum = hashlib.md5(data).hexdigest()
    return md5_checksum

# Define the owner of file
def get_file_owner(filepath):
    # Will it be OS-specific? Or do we need additional packegaes
    # return name for test purpose
    return "John"

# Define the creation date of a file
def get_creation_date(filepath):
    creation_time = os.path.getctime(filepath)
    formatted_date = datetime.datetime.fromtimestamp(creation_time).strftime('%Y-%m-%d %H:%M:%S')
    return formatted_date

# Set the "ingest_dir" to the  directory path 
# for the test purpose, I saved in the Desktop
ingest_dir = r"C:\Users\rexin\Desktop\docker"
done_dir = r"C:\Users\rexin\Desktop\docker_done"

# Create the directory if it does not exist
os.makedirs(ingest_dir, exist_ok=True)
os.makedirs(done_dir, exist_ok=True)

app = dash.Dash(__name__, suppress_callback_exceptions=True)


# # Establish the database connection and obtain the cursor
# connection, cursor = utils.get_oracle_cursor()

# # Example placeholder for db_col_names.
# db_col_names = utils.get_database_columns(cursor)

# Example placeholder for db_col_names. What is the name in the database?
db_col_names = ["Column1(can change later)", "Column2", "Column3"]

# Define functIon to create a new file list
def generate_new_file_list():
    new_files = [
        {"filename": "new_file_1.txt", "size": "10KB", "owner": "John Doe", "date": "2023-07-29"},
        {"filename": "new_file_2.txt", "size": "15KB", "owner": "Jane Smith", "date": "2023-07-30"},
        # Add more files as needed
    ]
    return new_files

# I have the code to generate new file at top, but for some reason it not works. so I add this file.
new_file_list = generate_new_file_list()

# Create a div to display the file info table
file_info_table_div = html.Div(id="file-info-table-container")

app.layout = html.Div(children=[
    dcc.Location(id='url', refresh=False),
    html.H1("Welcome to Your App"),

    html.Div([
        # Add the "Load All" button
        html.Button("Load All", id="load-all-button", n_clicks=0, style={"margin": "10px"}),

        # File list tables generated from the directories
        # Not sure why those table didn't show on the website
        html.Div([
            html.H3("New Files"),
            generate_file_list_template(ingest_dir, done_dir, new_file_list),
        ], id="new-file-list-container"),

        html.Div([
            html.H3("Done Files"),
            generate_file_list_template(ingest_dir, done_dir, done_file_list if 'done_file_list' in globals() else []),
        ], id="done-file-list-container"),

        dcc.Link("Go to Gen3 Portal", id="gen3-button", href="https://gen3.datacommons.io/login", style={"display": "block", "padding": "10px", "text-align": "center", "background-color": "#4CAF50", "color": "white", "text-decoration": "none", "border-radius": "5px", "margin": "10px auto"}),

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
                data=[],  # The actual data will be populated dynamically
                style_table={"border": "1px solid black"},
            ),
        ], id="file-list-container"),

        html.H3("Select file to upload"),

        # File selection dropdown
        dcc.Dropdown(
            id='file-dropdown',
            options=[{'label': filename, 'value': filename} for filename in os.listdir(ingest_dir)],
            placeholder="Select a file...",
            style={'margin': '10px'}
        ),

        # Load button
        html.Button("Load File", id="load-file-button", n_clicks=0, style={"margin": "10px"}),

        # Result box to show loaded file's information
        html.Div(id="result-box"),

        # Login form
        html.Div([
            html.H3("Login to Access the Gen3 Portal"),
            dcc.Input(id='username', type='text', placeholder='Enter username'),
            dcc.Input(id='password', type='password', placeholder='Enter password'),
            html.Button('Login', id='login-button', n_clicks=0)
        ], style={'margin': '20px'}),

        html.Div(
            id="container",
            children=[
                html.Div(
                    id="column-data",
                    children=[
                        html.H3(children="Database Table Columns"),
                        dash_table.DataTable(
                            id="database-columns-table",
                            columns=[{"name": "Column", "id": "Column"}],
                            data=[{"Column": col} for col in db_col_names]
                        ),
                        html.H3(children="Input Data Column"),
                        html.Div(
                            id="input-data-columns"
                        )
                    ]
                ),
                html.H1("Input Data"),
                html.Div(
                    id="table-container-input-data"
                )
            ]
        )
    ]),

    # Define the dcc.Store components to store the file lists
    dcc.Store(id="new-file-list-store", data=new_file_list),
    dcc.Store(id="done-file-list-store", data=done_file_list if 'done_file_list' in globals() else [])
])




# Define the function to move loaded files to the "done" directory
def move_loaded_files(loaded_files):
    for file_info in loaded_files:
        filepath = os.path.join(ingest_dir, file_info["filename"])
        os.rename(filepath, os.path.join(done_dir, file_info["filename"]))

@app.callback(
    [Output("table-container-input-data", "children"),
     Output("input-data-columns", "children"),
     Output("result-box", "children"),
     Output("file-list-table", "data")],
    [Input("upload-data", "contents"),
     Input("load-all-button", "n_clicks"),
     Input("load-file-button", "n_clicks")],
    [State('upload-data', 'filename'),
     State("ingest_dir", "children"),
     State("file-dropdown", "value"),
     State("file-list-table", "data")]
)
def handle_data_and_file_loading(contents, load_all_clicks, load_file_clicks, filename, ingest_dir, selected_filename, file_list_data):
    if not dash.callback_context.triggered:
        raise PreventUpdate

    trigger_id = dash.callback_context.triggered[0]["prop_id"]
    if trigger_id == "upload-data.contents":
        if contents is not None:
            content_string = contents.split(",")[1]
            decoded = base64.b64decode(content_string)
            global df

            try:
                if filename.endswith(".xlsx"):
                    df = pd.read_excel(io.BytesIO(decoded))
                elif filename.endswith(".csv"):
                    df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
            except Exception as e:
                print(e)
                return (
                    html.Div(["There was an error processing this file. Make sure the file is of type csv or xlsx!"]),
                    None,
                    None,
                    file_list_data
                )

            table = dash_table.DataTable(
                id='input-table',
                columns=[{"name": i, "id": i} for i in df.columns],
                data=df.to_dict('records'),
            )

            input_columns_table = dash_table.DataTable(
                id="input-columns-table",
                columns=[{"name": "Column", "id": "Column"}],
                data=[{"Column": col} for col in df.columns],
                editable=True
            )

            return table, input_columns_table, None, file_list_data

    elif trigger_id == "load-all-button.n_clicks":
        # Load all files from the "ingest_dir" directory and update the file list
        file_list = get_file_list(ingest_dir)
        return dash.no_update, dash.no_update, None, file_list

    elif trigger_id == "load-file-button.n_clicks":
        if not selected_filename:
            return (
                dash.no_update,
                dash.no_update,
                "Please select a file to upload.",
                file_list_data
            )

        # I need to add stuffs here to show how load the selected file into Gen3

        # Move the loaded file from "new" to "done" directory (simulating the move)
        file_info = next(item for item in file_list_data if item["filename"] == selected_filename)
        file_info["status"] = "Loaded"  # Add a status field to indicate loaded status

        # Return the updated file list and the loaded file's information
        return (
            dash.no_update,
            dash.no_update,
            html.Div([
                html.H4("File Loaded Successfully!"),
                html.P(f"Filename: {selected_filename}"),
                html.P(f"Size: {file_info['size']}"),
                html.P(f"Lines: {file_info['lines']}"),
                html.P(f"MD5 Checksum: {file_info['md5sum']}"),
                html.P(f"Owner: {file_info['owner']}"),
                html.P(f"Creation Date: {file_info['create_date']}")
            ]),
            file_list_data
        )

    raise PreventUpdate

@app.callback(
    [Output("new-file-list-table", "data"),
     Output("done-file-list-table", "data")],
    [Input("interval-component", "n_intervals")]
)
def update_file_lists(n_intervals):
    new_file_list, done_file_list = generate_file_list_template(ingest_dir, done_dir)
    return new_file_list, done_file_list

if __name__ == '__main__':
    # open on http://172.27.104.17:8050/
    app.run_server("0.0.0.0", debug=True)