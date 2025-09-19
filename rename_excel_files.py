import os
import shutil

# Define correct mapping of old name → new name
file_mapping = {
    "Age Analysis.xlsx": "age_analysis.xlsx",
    "Customer Account Parameters.xlsx": "customer_account_parameters.xlsx",
    "Customer Categories.xlsx": "customer_categories.xlsx",
    "Customer Regions.xlsx": "customer_regions.xlsx",
    "Customer.xlsx": "customer.xlsx",
    "Payment Header.xlsx": "payment_header.xlsx",
    "Payment Lines.xlsx": "payment_lines.xlsx",
    "Product Brands.xlsx": "product_brands.xlsx",
    "Product Categories.xlsx": "product_categories.xlsx",
    "Product Ranges.xlsx": "product_ranges.xlsx",
    "Products Styles.xlsx": "products_styles.xlsx",
    "Products.xlsx": "products.xlsx",
    "Purchases Headers.xlsx": "purchases_headers.xlsx",
    "Purchases Lines.xlsx": "purchases_lines.xlsx",
    "Representatives.xlsx": "representatives.xlsx",
    "Sales Header.xlsx": "sales_header.xlsx",
    "Sales Line.xlsx": "sales_line.xlsx",
    "Suppliers.xlsx": "suppliers.xlsx",
    "Trans Types.xlsx": "trans_types.xlsx"
}

DATA_DIR = "./exceldata"

# Rename each file
for old_name, new_name in file_mapping.items():
    old_path = os.path.join(DATA_DIR, old_name)
    new_path = os.path.join(DATA_DIR, new_name)
    
    if os.path.exists(old_path):
        try:
            os.rename(old_path, new_path)
            print(f"✅ Renamed: {old_name} → {new_name}")
        except Exception as e:
            print(f"❌ Error renaming {old_name}: {e}")
    else:
        print(f"⚠️  File not found: {old_name}")