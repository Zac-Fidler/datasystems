CREATE DATABASE warehouse;
use warehouse;

-- ----------------------------
-- Table structure for econ
-- ----------------------------

-- DROP TABLE IF EXISTS dim_econ;
CREATE TABLE dim_econ (
    country_name varchar(20) NOT NULL,
    quarterly_gdp_growth float(8) DEFAULT NULL,
    quarterly_inflation float(8) DEFAULT NULL,
    quarterly_unemployment float(8) DEFAULT NULL,
    quarterly_debt_gdp float(8) DEFAULT NULL,
    household_saving_ratio float(8) DEFAULT NULL,
    date datetime DEFAULT NULL,
    INDEX (date),
    CONSTRAINT PK_econ PRIMARY KEY (country_name, date)
);

-- ----------------------------
-- Table structure for stocks
-- ----------------------------
-- DROP TABLE IF EXISTS dim_stock;
CREATE TABLE dim_stock (
    stock_name varchar(20) NOT NULL,
    share_price float(8) DEFAULT NULL,
    share_quantity float(16) DEFAULT NULL,
    dividend float(8) DEFAULT NULL,
    earnings float(16) DEFAULT NULL,
    date datetime NOT NULL,
    INDEX (date),
    CONSTRAINT PK_stock PRIMARY KEY (stock_name, date)
);

-- -----------------------------
-- Table structure for main fact
-- -----------------------------
-- DROP TABLE IF EXISTS fact_comparison;
CREATE TABLE fact_comparison (
  country_name varchar(20) NOT NULL,
  stock_name varchar(20) NOT NULL,
  fact_stock_price_country_gdp_growth_coefficient float(8) DEFAULT NULL,
  fact_stock_price_country_inflation_coefficient float(8) DEFAULT NULL,
  fact_stock_price_country_unemployment_coefficient float(8) DEFAULT NULL,
  fact_stock_price_country_debt_gdp_coefficient float(8) DEFAULT NULL,
  fact_stock_price_country_household_savings_coefficient float(8) DEFAULT NULL,
  CONSTRAINT FK_country_name_comparison FOREIGN KEY (country_name) REFERENCES dim_econ(country_name),
  CONSTRAINT FK_stock_name_comparison FOREIGN KEY (stock_name) REFERENCES dim_stock(stock_name)
);

-- ------------------------------
-- Table structure for stock fact
-- ------------------------------
-- DROP TABLE IF EXISTS fact_stock_analysis;
CREATE TABLE fact_stock_analysis (
  stock_name varchar(20) NOT NULL,
  date datetime NOT NULL,
  fact_stock_growth float(8) DEFAULT NULL,
  fact_stock_dividend_price_ratio float(8) DEFAULT NULL,
  fact_stock_market_cap float(16) DEFAULT NULL,
  fact_stock_earnings_per_share float(8) DEFAULT NULL,
  fact_stock_earnings_price_ratio float(8) DEFAULT NULL,
  CONSTRAINT FK_stock_name_analysis FOREIGN KEY (stock_name) REFERENCES dim_stock(stock_name),
  CONSTRAINT FK_date_analysis FOREIGN KEY (date) REFERENCES dim_stock(date)
);