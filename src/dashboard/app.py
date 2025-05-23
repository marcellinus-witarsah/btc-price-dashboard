from src.timescaledb_ops import TimescaleDBOps
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd

app = dash.Dash(__name__)
app.title = "Real-Time BTC Chart"


def get_latest_ohlcv(granularity="10_seconds"):
    columns, rows = db_ops.read_data(f"btc_{granularity}_ohlcv", granularity)
    return pd.DataFrame(rows, columns=columns)


app.layout = html.Div(
    [
        html.H2(
            "📈 Real-Time BTC Candlestick Chart",
            style={
                "textAlign": "center",
                "fontFamily": "'Segoe UI', 'Roboto', 'Helvetica Neue', sans-serif",
                "color": "#2E86C1",
                "fontSize": "32px",
                "marginTop": "20px",
                "marginBottom": "30px",
                "letterSpacing": "1px",
            },
        ),
        dcc.Dropdown(
            id="granularity-dropdown",
            options=[
                {"label": "10 Seconds", "value": "10_seconds"},
                {"label": "20 Seconds", "value": "20_seconds"},
                {"label": "30 Seconds", "value": "30_seconds"},
            ],
            value="10_seconds",  # default value
            clearable=False,
            style={"width": "200px", "margin": "auto"},
        ),
        dcc.Graph(id="live-candle-chart"),
        dcc.Interval(id="interval-component", interval=10 * 1000, n_intervals=0),
    ]
)


@app.callback(
    Output("live-candle-chart", "figure"),
    Input("granularity-dropdown", "value"),
    Input("interval-component", "n_intervals"),
)
def update_chart(granularity, n):
    df = get_latest_ohlcv(granularity)
    fig = go.Figure()

    # Candlestick trace
    fig.add_trace(
        go.Candlestick(
            x=df["ts"],
            open=df["open_price"],
            high=df["high_price"],
            low=df["low_price"],
            close=df["close_price"],
            increasing=dict(
                line=dict(color="green", width=1),
                fillcolor="rgba(38, 166, 154, 1)",
            ),
            decreasing=dict(
                line=dict(color="red", width=1),
                fillcolor="rgba(239, 83, 80, 1)",
            ),
            name="OHLCV",
        )
    )

    fig.update_layout(
        xaxis_rangeslider_visible=False,
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        xaxis=dict(showgrid=False, showline=False),
        yaxis=dict(showgrid=True, gridcolor="lightgrey"),
        margin=dict(t=20, b=20, l=20, r=20),
    )

    return fig


if __name__ == "__main__":
    #############################################################
    # LOAD CONFIGURATION AND CONNECT TO TIMESCALEDB
    #############################################################
    config = load_config("src/timescaledb.ini", "timescaledb")
    db_ops = TimescaleDBOps(config)
    db_ops.connect()

    #############################################################
    # RUN SERVER
    #############################################################
    app.run_server(debug=True)

    #############################################################
    # SHUTDOWN CONNECTION TO TIMESCALEDB
    #############################################################
    db_ops.close_connection()
