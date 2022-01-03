import dash
from dash import html
from dash import dcc
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from dash.dependencies import Input, Output, State
import plotly.express as px
import dash_table
import sys


time_dict = {'1Day':'1','week':'7','month':'30','3month':'90','year':'365'}

def table_type(df_column):
    # Note - this only works with Pandas >= 1.0.0

    if sys.version_info < (3, 0):  # Pandas 1.0.0 does not support Python 2
        return 'any'

    if isinstance(df_column.dtype, pd.DatetimeTZDtype):
        return 'datetime',
    elif (isinstance(df_column.dtype, pd.StringDtype) or
            isinstance(df_column.dtype, pd.BooleanDtype) or
            isinstance(df_column.dtype, pd.CategoricalDtype) or
            isinstance(df_column.dtype, pd.PeriodDtype)):
        return 'text'
    elif (isinstance(df_column.dtype, pd.SparseDtype) or
            isinstance(df_column.dtype, pd.IntervalDtype) or
            isinstance(df_column.dtype, pd.Int8Dtype) or
            isinstance(df_column.dtype, pd.Int16Dtype) or
            isinstance(df_column.dtype, pd.Int32Dtype) or
            isinstance(df_column.dtype, pd.Int64Dtype)):
        return 'numeric'
    else:
        return 'any'


sqlEngine       = create_engine('mysql+pymysql://root:helloworld123@localhost', pool_recycle=3600)
dbConnection    = sqlEngine.connect()

###
fig_dropdown = dcc.Dropdown(
    id='dropdown_timeperiod',
    options=[
        {'label': 'Last Day', 'value': '1Day'},
        {'label': 'Last Week', 'value': 'week'},
        {'label': 'Last Month', 'value': 'month'},
        {'label': 'Last 3 Months', 'value': '3month'},
        {'label': 'Last Year', 'value': 'year'},
        {'label':'All Time', 'value': 'all_time'},

    ],
    style={'width':'50%'},
    value='month',
    clearable=False
)
###
def get_bargraph1(value):
    if(value=='all_time'):
        data = pd.read_sql("""select c.company_name company_name,count(t.tranx_id) count
                            from insider_trades.trades t, insider_trades.companies c
                            where t.company_id=c.company_id
                            group by c.company_name;""",dbConnection)
    else:
        data = pd.read_sql("""select c.company_name company_name,count(t.tranx_id) count
                                from insider_trades.trades t, insider_trades.companies c
                                where t.company_id=c.company_id
                                and  (t.date) > now()- interval """+ time_dict[value] +""" day
                                group by c.company_name;""",dbConnection)
    print(data)
    fig = px.bar(data,x='company_name',y='count',labels={'company_name':'Company Name','count':'# of trades'})
    return fig

#fig = px.bar(data,x='company_name',y='count')

################
def get_onclick_company_data(company_name,time_value):

    if (time_value == 'all_time'):
        data_2 = pd.read_sql("""select c.company_name,t.date,t.trader,t.type,t.quantity,t.short_val,t.quantity*t.short_val 'Transaction Value' 
                        from insider_trades.trades t, insider_trades.companies c
                        where t.company_id=c.company_id
                        and c.company_name='"""+company_name+"""'
                        """, dbConnection)
    else:
        data_2 = pd.read_sql("""select c.company_name,t.date,t.trader,t.type,t.quantity,t.short_val,t.quantity*t.short_val 'Transaction Value' 
                        from insider_trades.trades t, insider_trades.companies c
                        where t.company_id=c.company_id
                        and c.company_name='"""+company_name+"""'
                        and (t.date) > now()- interval  """+ time_dict[time_value] +""" day""", dbConnection)

    return data_2.to_dict('records')


################
data_2 = pd.read_sql("""select c.company_name,t.date,t.trader,t.type,t.quantity,t.short_val,t.quantity*t.short_val 'Transaction Value' 
                        from insider_trades.trades t, insider_trades.companies c
                        where t.company_id=c.company_id and (t.date) > now()- interval 20 day;""", dbConnection)


pd.set_option('display.expand_frame_repr', False)
#dbConnection.close()
app = dash.Dash(__name__)

############## LAYOUT #############
app.layout = html.Div(
    children=[
        html.H1(children="Insider Trade Stats",style={'textAlign':'center'}),
        fig_dropdown,
        dcc.Graph (id='bargraph1'),
        dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i, 'type':table_type(data_2[i])} for i in data_2.columns],
            data=data_2.to_dict('records'),
            filter_action='native',
            export_format='xlsx'
        ),
        html.P(id='placeholder')
    ]
)
##################################


@app.callback(
    Output('bargraph1', 'figure'),
    [Input('dropdown_timeperiod', 'value'),]
    
)

def update_graph(value):
    if value=='1Day':
        print("time=24")
        return get_bargraph1(value)
    elif value=='week':
        print("time=1w")
        return  get_bargraph1(value)
    elif value=='month':
        print("time=1m")
        return get_bargraph1(value)
    elif value=='3month':
        return get_bargraph1(value)
    elif value=='year':
        return get_bargraph1(value)
    elif value == 'all_time':
        return get_bargraph1(value)

@app.callback(
    Output('table','data'),
    Input('bargraph1','clickData'),
    State('dropdown_timeperiod', 'value')
)
def display_click_data(clickData, value):
    if not clickData:
        raise dash.exceptions.PreventUpdate
    x = clickData["points"][0]['x']
    print(value)
    fig = get_onclick_company_data(x,value)
    return fig



if __name__ == "__main__":
    app.run_server(debug=True)

 

 

