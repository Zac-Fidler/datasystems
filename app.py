import pandas as pd
from dash import Dash, Input, Output, dcc, html, dash_table

data = (
    pd.read_csv("./csv_azure/stocks.csv")
    #.query("stock_name == 'BHP'")
    .assign(Date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
    .sort_values(by="Date")
)
Stocks = data["stock_name"].sort_values().unique()


#TO DO
econ_data = (
    pd.read_csv("./csv_azure/econ.csv")
    #.query("stock_name == 'BHP'")
    .assign(date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
    .sort_values(by="date")
)
Economy = econ_data["country_name"].sort_values().unique()

fact_comparison_data = (
    pd.read_csv("./csv_azure/fact_comparison.csv")
    #.query("stock_name == 'BHP'")
    #.assign(Date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
    #.sort_values(by="Date")
)
#fact_comparison = fact_comparison_data["stock_name"].sort_values().unique()

fact_stock_analysis_data = (
    pd.read_csv("./csv_azure/fact_stock_analysis.csv")
    #.query("stock_name == 'BHP'")
    .assign(date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
    .sort_values(by="date")
)
#fact_stock_analysis = fact_stock_analysis_data["stock_name"].sort_values().unique()


external_stylesheets = [
    {
        "href":(
            "style.css"
        ),
        "rel": "stylesheet",
    },
]
app = Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "Stocklens"


app.layout = html.Div(
    children=[
        html.Div(
            children=[
                html.H1(children="Stocklens",
                        className="header-title"
                ),
                html.P(
                    children=(
                        "Analyze the behavior of stocks correlation to economic data"
                        "of stocks on the ASX between 2007 and 2012"
                    ),
                    className="header-description"
                ),
            ],
            className="header"
        ),
        html.Div(
            children=[
                html.Div(
                    children=[
                        html.Div(children="Economy", className="menu-title"),
                        dcc.Dropdown(
                            id="econ-filter",
                            options=[
                                {"label": e, "value": e}
                                for e in Economy
                            ],
                            value="AU",
                            clearable=False,
                            className="dropdown",
                            style={'minWidth': 200},
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.Div(children="Stocks", className="menu-title"),
                        dcc.Dropdown(
                            id="stocks-filter",
                            options=[
                                {"label": stock, "value": stock}
                                for stock in Stocks
                            ],
                            value="BHP",
                            clearable=False,
                            className="dropdown",
                            style={'minWidth': 200},
                        ),
                    ]
                ),
            ],
            className="menu",
        ),
        html.Div(
            children=[
                html.Div(
                    children=[
                        dcc.Graph(
                            id="price-chart",
                            config={"displayModeBar": False},
                        ),
                    ],
                    className="card"
                ),
                html.Div(
                    children=[
                        dcc.Graph(
                            id="dividend-chart",
                            config={"displayModeBar": False},
                        ),
                    ],
                    className="card"
                ),
                html.Div(
            html.Div(
                children= dash_table.DataTable(
                    id="table",
                    columns=[{"name": i, "id": i} for i in econ_data.columns],
                    #sorting=True,
                    #sorting_type='multi',
                    #sorting_settings=[],
                    ),
                #className="card"
            ),
        ),
            ],
            className="wrapper"
        ),
        #html.Div(id="tables-container"),
        
    ]
)

@app.callback(
    Output("price-chart", "figure"),
    Output("dividend-chart", "figure"),
    Input("stocks-filter", "value"),
    Input("econ-filter", "value"),
)
def update_charts(stock, economy):
    filtered_data = data.query(
        "stock_name == @stock"
    )
    price_chart_figure = {
        "data": [
            {
                "x": filtered_data["date"],
                "y": filtered_data["share_price"],
                "type": "lines",
                "hovertemplate": "$%{y:.2f}<extra></extra>",
            },
        ],
        "layout": {
            "title": {
                "text": "Stock Price by Day",
                "x": 0.05,
                "xanchor": "left",
            },
            "xaxis": {"fixedrange": True},
            "yaxis": {"tickprefix": "$", "fixedrange": True},
            "colorway": ["#17B897"],
        },
    }

    dividend_chart_figure = {
        "data": [
            {
                "x": filtered_data["date"],
                "y": filtered_data["dividend"],
                "type": "lines",
                "hovertemplate": "$%{y:.2f}<extra></extra>",
            },
        ],
        "layout": {
            "title": {"text": "Stock Dividend", "x": 0.05, "xanchor": "left"},
            "xaxis": {"fixedrange": True},
            "yaxis": {"fixedrange": True},
            "colorway": ["#E12D39"],
        },
    }
    return price_chart_figure, dividend_chart_figure


if __name__ == "__main__":
    app.run_server(debug=True)