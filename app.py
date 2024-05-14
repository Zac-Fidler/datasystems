import pandas as pd
from dash import Dash, Input, Output, dcc, html, dash_table
import sqlalchemy as sa
from sqlalchemy import create_engine, text


connection_url = sa.engine.URL.create(
    drivername="mysql+pymysql",
    username="root",
    password="",
    host="localhost",
    database="warehouse",
)

engine = create_engine(connection_url)

#data = pd.read_sql_query(sql='SELECT * FROM dim_stock',con=engine)
data = pd.DataFrame(engine.connect().execute(text('SELECT * FROM dim_stock')))

#data = (
#    pd.read_csv("./csv_processed/stocks.csv")
#    #.query("stock_name == 'BHP'")
#    .assign(Date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
#    .sort_values(by="Date")
#)
Stocks = data["stock_name"].sort_values().unique()


#TO DO
#econ_data = (
#    pd.read_csv("./csv_processed/dim_econ.csv")
#    #.query("stock_name == 'BHP'")
#    .assign(date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
#    .sort_values(by="date")
#)

econ_data = pd.DataFrame(engine.connect().execute(text('SELECT * FROM dim_econ')))
Economy = econ_data["country_name"].sort_values().unique()

#fact_comparison_data = (
#    pd.read_csv("./csv_processed/fact_comparison.csv")
    #.query("stock_name == 'BHP'")
    #.assign(Date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
    #.sort_values(by="Date")
#)
fact_comparison_data = pd.DataFrame(engine.connect().execute(text('SELECT * FROM fact_comparison')))
#fact_comparison = fact_comparison_data["stock_name"].sort_values().unique()

#fact_stock_analysis_data = (
#    pd.read_csv("./csv_processed/fact_stock_analysis.csv")
#    #.query("stock_name == 'BHP'")
#    .assign(date=lambda data: pd.to_datetime(data["date"], format="%d/%m/%Y %H:%M:%S"))
#    .sort_values(by="date")
#)
fact_stock_analysis_data = pd.DataFrame(engine.connect().execute(text('SELECT * FROM fact_stock_analysis')))
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
                    html.Div(
                        id="fact-comparison-table",
                        className="card"
                    ),
                ),
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
                            id="market-cap-chart",
                            config={"displayModeBar": False},
                        ),
                    ],
                    className="card"
                ),
            ],
            className="wrapper"
        ),
        #html.Div(id="tables-container"),
        
    ]
)

@app.callback(
    Output("price-chart", "figure"),
    Output("market-cap-chart", "figure"),
    Output("fact-comparison-table", "children"),
    Input("stocks-filter", "value"),
    Input("econ-filter", "value"),
    #Input("fact-comparison-table", "column")
)
def update_charts(stock, economy):
    filtered_data = data.query(
        "stock_name == @stock"
    )

    analysis_data = fact_stock_analysis_data.query(
        "stock_name == @stock"
    ) 


    comparison_data = fact_comparison_data.query(
        "country_name == @economy & stock_name == @stock"
    )

    #table_data = comparison_data.drop(
    #    columns=["stock_name"]
    #)

    table_data = comparison_data.rename(
        columns={
        'country_name': 'Country',
        'stock_name': 'Stock',
        'fact_stock_price_country_gdp_growth_coefficient': 'Stock Price GDP Coeff.',
        'fact_stock_price_country_inflation_coefficient':'Stock Price Inflation Coeff.',
        'fact_stock_price_country_unemployment_coefficient':'Stock Price Unemployment Coeff.',
        'fact_stock_price_country_debt_gdp_coefficient':'Stock Price Debt to GDP Coeff.',
        'fact_stock_price_country_household_savings_coefficient':'Stock Price Household Savings Coeff.'
        }
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

    market_cap_chart_figure = {
        "data": [
            {
                "x": analysis_data["date"],
                "y": analysis_data["fact_stock_market_cap"],
                "type": "lines",
                "hovertemplate": "$%{y:.2f}<extra></extra>",
            },
        ],
        "layout": {
            "title": {"text": "Market Cap", "x": 0.05, "xanchor": "left"},
            "xaxis": {"fixedrange": True},
            "yaxis": {"fixedrange": True},
            "colorway": ["#E12D39"],
        },
    }

    fact_comparison_table = html.Div(
        children=[
            dash_table.DataTable(
                columns=[{"name": i, "id": i} for i in table_data.columns],
                data= table_data.to_dict('records'),
                style_cell={'whiteSpace': 'normal', 'fontFamily': 'helvetica'},
            )
        ]
    )

    return price_chart_figure, market_cap_chart_figure, fact_comparison_table


if __name__ == "__main__":
    app.run_server(debug=True)