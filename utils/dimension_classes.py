from utils.setup import *
import pandas as pd

# Load ENV variables from file
load_dotenv()
storage_account = os.environ.get("ACCOUNT_STORAGE")
container_name = os.environ.get("CONTAINER_NAME")

# Raw data filename variables
stock_data_filename = "stock_data.csv"
econ_data_filename = "econ_data.csv"

# Call setup functions
azureDB = AzureDB()
azureDB.access_container(container_name)

# Upload or access raw CSV data
blob_list = azureDB.list_blobs()
print("Checking if raw data uploaded to Azure")
if len(blob_list) < 2: 
    # Upload files if not there
    print("Raw data not found, uploading now.")
    azureDB.upload_blob(stock_data_filename)
    azureDB.upload_blob(econ_data_filename)
    
stock_df = azureDB.access_blob_csv(blob_name=stock_data_filename)
econ_df  = azureDB.access_blob_csv(blob_name=econ_data_filename)

print(stock_df.head())
print(econ_df.head())

class ModelAbstract():
    def __init__(self):
        self.columns = None
        self.dimension_table = None
        
    def dimension_generator(self, name: str, columns: list, df: pd.DataFrame):
        dim = df[columns]
        dim = dim.drop_duplicates
        
        # creating primary key for dimension table
        dim.insert(loc=0, column=f"{name}_id", value=range(1, len(dim) + 1))
        self.dimensional_table = dim
        self.name = name
        self.columns = columns
        
        # dim.to_csv(self.name + ".csv")

class DimCountry(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("Country", ["country_name"], econ_df)

class DimGDPGrowth(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("GDPGrowth", ["quarterly_GDP_growth"], econ_df)
        
class DimInflation(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("Inflation", ["quarterly_inflation"], econ_df)
                
class DimUnemployment(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("Unemployment", ["quarterly_unemployment"], econ_df)
                
class DimDebtGDP(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("DebtGDP", ["quarterly_debt_GDP"], econ_df)
                
class DimHouseholdSavingRatio(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("HouseholdSavingRatio", ["household_saving_ratio"], econ_df)
                
class DimStockName(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("StockName", ["stock_name"], stock_df)
                
class DimSharePrice(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("SharePrice", ["share_price"], stock_df)
                
class DimShareQuantity(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("ShareQuantity", ["share_quantity"], stock_df)
                
class DimDividend(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("Dividend", ["dividend"], stock_df)
                
class DimEarnings(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("Earnings", ["earnings"], stock_df)
        
class DimDate(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator("Date", ["date"], stock_df)
        
        