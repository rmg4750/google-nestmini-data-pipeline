from flask import Flask, render_template 
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import os
import plotly.express as px

app = Flask(__name__)

dash_app = Dash(__name__, server=app, url_base_pathname='/dashboard/')

accumulated_data = pd.DataFrame()

def load_data():
    global accumulated_data
    data_path = "C:/Users/Ryan/Documents/GoogleNestMLPipeline/data/processed/traffic_volume.csv"
    
    if os.path.exists(data_path):
        df = pd.read_csv(data_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        accumulated_data = pd.concat([accumulated_data, df])
        
        time_threshold = accumulated_data['timestamp'].max() - pd.Timedelta(hours=1)
        accumulated_data = accumulated_data[accumulated_data['timestamp'] >= time_threshold]
        
        return accumulated_data
    else:
        return accumulated_data

@dash_app.callback(
    Output('traffic-time-series', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_graph(n):
    df = load_data()

    if df.empty:
        return {
            "layout": {
                "title": "No data available",
                "xaxis": {"title": "Timestamp"},
                "yaxis": {"title": "Traffic Volume"},
                "paper_bgcolor": "#333",  
                "plot_bgcolor": "#333",  
                "font": {"color": "#fff"}
            }
        }

    end_time = df['timestamp'].max()
    start_time = end_time - pd.Timedelta(hours=0.5)

    fig = px.line(
        df, 
        x='timestamp', 
        y='total_packets',
        title="Google Nest Mini Traffic Volume",
        template="plotly_dark"
    )

    fig.update_xaxes(range=[start_time, end_time])
    fig.update_yaxes(range=[0, 50] )

    return fig

dash_app.layout = html.Div(
    children=[
        dcc.Graph(
            id='traffic-time-series',
            style={'backgroundColor': '#1e1e1e'}
        ),  
        dcc.Interval(
            id='interval-component',
            interval=30*1000, 
            n_intervals=0
        )
    ],
    style={
        'position': 'absolute',
        'top': '10px',
        'left': '10px',
        'width': '35%',
        'height': 'auto',
        'padding': '10px',
        'border': '1px solid #444',  
        'backgroundColor': '#333', 
        'color': '#fff'  
    }
)

@app.route('/')
def home():
    return render_template('index.html')

if __name__ == "__main__":
    app.run(debug=True)


