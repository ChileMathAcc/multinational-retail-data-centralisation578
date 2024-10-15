import data_cleaning
import data_extraction
import database_utils
import pandas as pd

D = data_cleaning.data_cleaner.convert_product_weights()
print(D)