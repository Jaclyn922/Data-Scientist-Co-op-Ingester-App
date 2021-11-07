import dash
from dash import dcc
from dash import html
import dash.dash_table as dash_table
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import plotly.express as px
import pandas as pd
import io
import configparser
import base64
import utils

app = dash.Dash(__name__)

connection, cursor = utils.get_oracle_cursor()

db_col_names = utils.get_database_columns(cursor)

app.layout = html.Div(children=[
    html.H3("Select file to upload"),
    dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Drag and Drop or ',
            html.A('Select Files')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        # Allow multiple files to be uploaded
        multiple=False
    ),
    html.Div(
        id="container",
        children=[
            html.Div(
                id="column-data",
                children=[
                    html.H3(children="Database Table Columns"),
                    dash_table.DataTable(
                        columns=[{"name": "Column", "id": "Column"}],
                        data=[{"Column": col} for col in db_col_names]),
                    html.H3(children="Input Data Column"),
                    html.Div(
                        id="input-data-columns"
                    )
                ]
            ),
            html.H1("Input Data"),
            html.Div(
                id="table-container"
            )
        ])

])

# handles loading in data
@app.callback(
    Output(component_id="table-container", component_property="children"),
    Output(component_id="input-data-columns", component_property="children"),
    Input(component_id="upload-data", component_property="contents"),
    State('upload-data', 'filename')
)
def load_data(contents, filename):
    
    if contents:
        content_string = contents.split(",")[1]
        decoded = base64.b64decode(content_string)
        global df

        try:
            if filename.endswith(".xlsx"):
                df = pd.read_excel(io.BytesIO(decoded))
            elif filename.endswith(".csv"):
                df = pd.read_csv(
                    io.StringIO(decoded.decode('utf-8')))
        except Exception as e:
            print(e)
            return html.Div(["There was an error processing this file. Make sure the file is or type csv of xlsx!"])

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

        return table, input_columns_table
    else:
        raise PreventUpdate
        

if __name__ == '__main__':
    # open on http://172.27.104.17:8050/
    app.run_server("0.0.0.0", debug=True)
    
    