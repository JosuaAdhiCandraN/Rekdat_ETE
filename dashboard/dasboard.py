import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import psycopg2

# Fungsi  fetch data dari PostgreSQL
def fetch_sensor_data():
    conn = psycopg2.connect(
        host="localhost",
        database="database",
        user="username",
        password="password"
    )
    query = "SELECT timestamp, temperature, humidity FROM database"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Inisialisasi aplikasi Dash
app = dash.Dash(__name__)

# Layout dashboard
app.layout = html.Div([
    html.H1("Dashboard Sensor Suhu dan Kelembapan"), 
    dcc.Dropdown(
        id='dropdown-jenis-data',
        options=[
            {'label': 'Temperature', 'value': 'temperature'},
            {'label': 'Humidity', 'value': 'humidity'}
        ],
        value='temperature',
        clearable=False
    ),
    
    
    dcc.Graph(id='chart-sensor'),
])

@app.callback(
    Output('chart-sensor', 'figure'),
    [Input('dropdown-jenis-data', 'value')]
)
def update_chart(selected_data):
    # Ambil data terbaru dari PostgreSQL
    df = fetch_sensor_data()
    fig = px.line(
        df,
        x='timestamp',
        y=selected_data,
        title=f'{selected_data.capitalize()} Over Time',
        labels={'timestamp': 'Timestamp ', selected_data: selected_data.capitalize()},
        template='plotly_dark'
    )
    return fig

# Jalankan aplikasi
if __name__ == '__main__':
    app.run_server(debug=True)