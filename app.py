from flask import Flask, render_template 
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import os
import plotly.express as px

# Initialize the Flask app
app = Flask(__name__)

# Initialize Dash app within Flask
dash_app = Dash(__name__, server=app, url_base_pathname='/dashboard/')

# Global DataFrame to accumulate data
accumulated_data = pd.DataFrame()

# Function to load the latest data
def load_data():
    global accumulated_data
    data_path = "C:/Users/Ryan/Documents/GoogleNestMLPipeline/data/processed/traffic_volume.csv"
    
    if os.path.exists(data_path):
        df = pd.read_csv(data_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])  # Ensure timestamp is a datetime object
        
        # Concatenate new data with existing accumulated data
        accumulated_data = pd.concat([accumulated_data, df])
        
        # Optional: Remove data older than 1 hour
        time_threshold = accumulated_data['timestamp'].max() - pd.Timedelta(hours=1)
        accumulated_data = accumulated_data[accumulated_data['timestamp'] >= time_threshold]
        
        return accumulated_data
    else:
        return accumulated_data  # Return the accumulated data if the file doesn't exist yet

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
                "paper_bgcolor": "#333",  # Dark background
                "plot_bgcolor": "#333",   # Dark plot area background
                "font": {"color": "#fff"}  # White text
            }
        }

    # Get the most recent timestamp in the data
    end_time = df['timestamp'].max()
    start_time = end_time - pd.Timedelta(hours=0.5)

    fig = px.line(
        df, 
        x='timestamp', 
        y='total_packets',  # Correct column name
        title="Google Nest Mini Traffic Volume",
        template="plotly_dark"  # Use dark theme
    )

    fig.update_xaxes(range=[start_time, end_time])
    fig.update_yaxes(range=[0, 50] )

    return fig

dash_app.layout = html.Div(
    children=[
        dcc.Graph(
            id='traffic-time-series',
            style={'backgroundColor': '#1e1e1e'}  # Dark background for the graph
        ),  
        dcc.Interval(
            id='interval-component',
            interval=30*1000,  # Adjust to your data update frequency in milliseconds
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
        'border': '1px solid #444',  # Darker border for dark theme
        'backgroundColor': '#333',  # Dark background color
        'color': '#fff'  # Text color for dark theme
    }
)

@app.route('/')
def home():
    return render_template('index.html')

if __name__ == "__main__":
    app.run(debug=True)


