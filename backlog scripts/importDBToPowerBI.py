import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["clearvue"]


# WE ARE CREATING A FACT TABLE FROM THE DIMENTION TABLES
#--------------------------------------------------------------------
products = pd.DataFrame(list(db["products"].find()))
#related by DOC NUMBER
sales_line = pd.DataFrame(list(db["sales line"].find()))
sales_header = pd.DataFrame(list(db["sales header"].find()))
#related by transtypecode
trans_types = pd.DataFrame(list(db["trans types"].find()))
#--------------------------------------------------------------------
products_styles = pd.DataFrame(list(db["products styles"].find()))
#prodbra code
product_brands = pd.DataFrame(list(db["product brands"].find()))
product_categories = pd.DataFrame(list(db["product categories"].find()))
product_ranges = pd.DataFrame(list(db["product ranges"].find()))
purchases_lines = pd.DataFrame(list(db["purchases lines"].find()))

#--------------------------------------------------------------------
#related by SUPPLIER_CODE
purchases_headers = pd.DataFrame(list(db["purchases headers"].find()))

suppliers = pd.DataFrame(list(db["suppliers"].find()))
#--------------------------------------------------------------------
#related by rep code
representatives =  pd.DataFrame(list(db["representatives"].find()))
#related by region code
customer = pd.DataFrame(list(db["customer"].find()))
#related by ccat_code
customer_categories = pd.DataFrame(list(db["customer categories"].find()))
customer_regions =  pd.DataFrame(list(db["Customer Regions"].find()))
#related by customer number
customer_account_parameters = pd.DataFrame(list(db["customer account parameters"].find())) 
#related by customer number
age_analysis = pd.DataFrame(list(db["age analysis"].find())) 
#related by customer number
payment_lines = pd.DataFrame(list(db["payment lines"].find()))
#related by customer number 
payment_header = pd.DataFrame(list(db["payment header"].find()))
#--------------------------------------------------------------------
# print(len(payment_header))
# print(len(payment_lines))
# print(len(age_analysis))
# print(len(customer_account_parameters))
# print(len(customer_regions))
# print(len(customer_categories))
# print(len(customer))
# print(len(representatives))
# print(len(suppliers))
# print(len(purchase_headers))

# print(len(purchase_lines))
# print(len(product_ranges))
# print(len(product_categories))
# print(len(product_brands))
# print(len(products_styles))
# print(len(trans_types))
# print(len(sales_header))
# print(len(sales_line))
# print(len(products))

# HOW IS MERGING DONE?
# YOU MERGE BY THE MOST RELATED KEY IN EACH TABLES.

#FIRST MERGE
# Merge example (make sure INVENTORY_CODE exists in both collections)
if ("INVENTORY_CODE" in products.columns and 
    "INVENTORY_CODE" in products_styles.columns and 
    "INVENTORY_CODE" in sales_line.columns and
    "DOC_NUMBER" in sales_header.columns):

    # First merge: products + products_styles
    temp = pd.merge(
        products,
        products_styles,
        on="INVENTORY_CODE",
        how="left"
    )

    # Second merge: sales header + sales_line  
    temp2 = pd.merge(
        sales_header,
        sales_line,
        on="DOC_NUMBER",
        how="left"
    )

    #Third Merge: temp + temp2
    combined = pd.merge(temp,
                        temp2,
                        on="INVENTORY_CODE",
                        how="left")

else:
    print("fell back 🥲")
    combined = products  # fallback if join key is missing

# Return dataset for Power BI
dataset = combined


#SECOND MERGE
if ("PURCH_DOC_NO" in purchases_headers.columns and 
    "PURCH_DOC_NO" in purchases_lines.columns and
    "SUPPLIER_CODE" in suppliers.columns):
    
    temp = pd.merge(purchases_headers,
                    purchases_lines,
                    on="PURCH_DOC_NO",
                    how="left")
    
    temp2 = pd.merge(temp,
                     suppliers,
                     on="SUPPLIER_CODE",
                     how="right")
else:
    print("fell back 2")

dataset = temp2

#THIRD MERGE 
if("CUSTOMER_NUMBER" in customer_account_parameters.columns and 
   "CUSTOMER_NUMBER" in customer.columns and
   "CUSTOMER_NUMBER" in payment_lines.columns and 
   "CUSTOMER_NUMBER" in payment_header.columns):
    
    temp = pd.merge(customer_account_parameters,
                    customer,
                    on="CUSTOMER_NUMBER",
                    how="inner")
   
    
    temp2 = pd.merge(payment_lines,
                     payment_header,
                     on="CUSTOMER_NUMBER",
                     how="left"
                    )

    
    combined = pd.merge(temp,
                        temp2,
                        on="CUSTOMER_NUMBER",
                        how="left"
                    )

dataset=combined
print(dataset)

#4th MERGE





