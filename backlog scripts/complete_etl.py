#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ClearVue Ltd. - NoSQL BI System ETL Pipeline (Excel Version)
Author: [Your Name]
Date: 2025-09-10
Description: Ingests 17 Excel (.xlsx) source files, applies FY logic, loads into MongoDB.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pymongo import MongoClient
import logging
import os
import sys

# ===========================
# CONFIGURATION
# ===========================

MONGO_URI = "mongodb://localhost:27017/"  # 🔑 REPLACE WITH YOUR URI
DB_NAME = "clearvue_bi"
COLLECTION_NAME = "sales_fact"

DATA_DIR = "./exceldata"  # 📁 Folder containing your 17 .xlsx files

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ===========================
# FINANCIAL YEAR LOGIC (UNCHANGED)
# ===========================

def get_last_saturday_of_month(year, month):
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    offset = (last_day.weekday() - 5) % 7
    return last_day - timedelta(days=offset)

def get_last_friday_of_month(year, month):
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    offset = (last_day.weekday() - 4) % 7
    return last_day - timedelta(days=offset)

def assign_financial_period(sample_date):
    if pd.isna(sample_date):
        return None, None, None, None, None

    dt = sample_date if isinstance(sample_date, datetime) else pd.to_datetime(sample_date)
    year = dt.year
    month = dt.month

    if month == 1:
        fm_start = get_last_saturday_of_month(year - 1, 12)
    else:
        fm_start = get_last_saturday_of_month(year, month - 1)

    fm_end = get_last_friday_of_month(year, month)
    financial_year = fm_start.year

    if month == 2:
        fm_number = 1
    elif month > 2:
        fm_number = month - 1
    else:
        fm_number = 12
        financial_year -= 1

    fm_quarter_map = {1:1, 2:1, 3:1, 4:2, 5:2, 6:2, 7:3, 8:3, 9:3, 10:4, 11:4, 12:4}
    financial_quarter = fm_quarter_map.get(fm_number, 1)

    return financial_year, fm_number, financial_quarter, fm_start, fm_end

def fin_period_to_clearvue_fy(fin_period):
    try:
        year = fin_period // 100
        month = fin_period % 100
        sample_date = datetime(year, month, 15)
        fy, fm, fq, _, _ = assign_financial_period(sample_date)
        return fy, fm, fq
    except:
        return None, None, None

# ===========================
# DATA LOADING & TRANSFORMATION — EXCEL VERSION
# ===========================

def load_data():
    """Load all 17 Excel source files into DataFrames."""
    logger.info("📂 Loading Excel source files...")

    files = {
        'sales_header': 'sales_header.xlsx',
        'sales_line': 'sales_line.xlsx',
        'products': 'products.xlsx',
        'product_categories': 'product_categories.xlsx',
        'product_brands': 'product_brands.xlsx',
        'product_ranges': 'product_ranges.xlsx',
        'products_styles': 'products_styles.xlsx',
        'customer': 'customer.xlsx',
        'customer_regions': 'customer_regions.xlsx',
        'representatives': 'representatives.xlsx',
        'trans_types': 'trans_types.xlsx',
        'payment_lines': 'payment_lines.xlsx',
        'payment_header': 'payment_header.xlsx',
        'age_analysis': 'age_analysis.xlsx',
        'customer_account_parameters': 'customer_account_parameters.xlsx',
        'purchases_headers': 'purchases_headers.xlsx',
        'purchases_lines': 'purchases_lines.xlsx',
        'suppliers': 'suppliers.xlsx'
    }

    dfs = {}
    for name, filename in files.items():
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            logger.warning(f"⚠️  File not found: {filepath}")
            continue

        try:
            # Try reading with openpyxl (default for .xlsx)
            dfs[name] = pd.read_excel(filepath, engine='openpyxl')
            logger.info(f"✅ Loaded {name} with {len(dfs[name])} rows")
        except Exception as e:
            logger.warning(f"⚠️  Failed with openpyxl for {filename}: {e}. Trying xlrd...")
            try:
                # Fallback to xlrd (for older .xls or some .xlsx)
                dfs[name] = pd.read_excel(filepath, engine='xlrd')
                logger.info(f"✅ Loaded {name} with xlrd engine")
            except Exception as e2:
                logger.error(f"❌ Failed to load {filename} with any engine: {e2}")

    return dfs

def transform_sales_data(dfs):
    """Join sales data with dimensions and apply FY logic."""
    logger.info("🔗 Joining sales data with dimensions...")

    if 'sales_line' not in dfs or 'sales_header' not in dfs:
        logger.error("❌ Required sales data missing!")
        return None

    merged = dfs['sales_line'].merge(dfs['sales_header'], on='DOC_NUMBER', how='inner')
    logger.info(f"   → After sales_header join: {len(merged)} rows")

    # Add products
    if 'products' in dfs:
        prod_cols = ['INVENTORY_CODE', 'PRODCAT_CODE', 'LAST_COST']
        prod_cols = [c for c in prod_cols if c in dfs['products'].columns]
        merged = merged.merge(dfs['products'][prod_cols], on='INVENTORY_CODE', how='left')
        logger.info("   → Added products")

    # Add product categories + brands + ranges
    if 'product_categories' in dfs:
        cat_cols = ['PRODCAT_CODE', 'PRODCAT_DESC', 'BRAND_CODE', 'PRAN_CODE']
        cat_cols = [c for c in cat_cols if c in dfs['product_categories'].columns]
        merged = merged.merge(dfs['product_categories'][cat_cols], on='PRODCAT_CODE', how='left')

        if 'product_brands' in dfs:
            brand_cols = ['PRODBRA_CODE', 'PRODBRA_DESC']
            brand_cols = [c for c in brand_cols if c in dfs['product_brands'].columns]
            merged = merged.merge(dfs['product_brands'][brand_cols], left_on='BRAND_CODE', right_on='PRODBRA_CODE', how='left')

        if 'product_ranges' in dfs:
            range_cols = ['PRAN_CODE', 'PRAN_DESC']
            range_cols = [c for c in range_cols if c in dfs['product_ranges'].columns]
            merged = merged.merge(dfs['product_ranges'][range_cols], on='PRAN_CODE', how='left')
        logger.info("   → Added product categories/brands/ranges")

    # Add product styles
    if 'products_styles' in dfs:
        style_cols = ['INVENTORY_CODE', 'GENDER', 'MATERIAL', 'STYLE']
        style_cols = [c for c in style_cols if c in dfs['products_styles'].columns]
        merged = merged.merge(dfs['products_styles'][style_cols], on='INVENTORY_CODE', how='left')
        logger.info("   → Added product styles")

    # Add customer info
    if 'customer' in dfs:
        cust_cols = ['CUSTOMER_NUMBER', 'REGION_CODE', 'REP_CODE', 'CREDIT_LIMIT', 'CCAT_CODE']
        cust_cols = [c for c in cust_cols if c in dfs['customer'].columns]
        merged = merged.merge(dfs['customer'][cust_cols], on='CUSTOMER_NUMBER', how='left')
        logger.info("   → Added customer info")

    # Add customer regions
    if 'customer_regions' in dfs:
        region_cols = ['REGION_CODE', 'REGION_DESC']
        region_cols = [c for c in region_cols if c in dfs['customer_regions'].columns]
        merged = merged.merge(dfs['customer_regions'][region_cols], on='REGION_CODE', how='left')
        logger.info("   → Added customer regions")

    # Add representatives
    if 'representatives' in dfs:
        rep_cols = ['REP_CODE', 'REP_DESC']
        rep_cols = [c for c in rep_cols if c in dfs['representatives'].columns]
        merged = merged.merge(dfs['representatives'][rep_cols], on='REP_CODE', how='left')
        logger.info("   → Added representatives")

    # Add transaction types
    if 'trans_types' in dfs:
        trans_cols = ['TRANSTYPE_CODE', 'TRANSTYPE_DESC']
        trans_cols = [c for c in trans_cols if c in dfs['trans_types'].columns]
        merged = merged.merge(dfs['trans_types'][trans_cols], on='TRANSTYPE_CODE', how='left')
        logger.info("   → Added transaction types")

    # Fill NaNs
    fill_values = {
        'PRODCAT_DESC': 'Unknown', 'PRODBRA_DESC': 'Unknown', 'PRAN_DESC': 'Unknown',
        'GENDER': 'Unknown', 'MATERIAL': 'Unknown', 'STYLE': 'Unknown',
        'REGION_DESC': 'Unknown', 'REP_DESC': 'Unknown', 'TRANSTYPE_DESC': 'Unknown',
        'LAST_COST': 0, 'CREDIT_LIMIT': 0, 'CCAT_CODE': 0
    }
    for col, val in fill_values.items():
        if col in merged.columns:
            merged[col] = merged[col].fillna(val)

    # Apply Financial Year Logic
    logger.info("📅 Applying ClearVue Financial Year logic...")
    fy_results = merged['FIN_PERIOD'].apply(
        lambda fp: fin_period_to_clearvue_fy(int(fp)) if pd.notna(fp) and str(fp).isdigit() else (None, None, None)
    )
    merged[['financial_year', 'financial_month', 'financial_quarter']] = pd.DataFrame(fy_results.tolist(), index=merged.index)

    logger.info(f"📊 Final merged dataset: {len(merged)} rows")
    return merged

def create_mongo_documents(df):
    """Convert DataFrame rows to MongoDB sales_fact documents."""
    logger.info("📄 Creating MongoDB documents...")

    docs = []
    for _, row in df.iterrows():
        try:
            doc = {
                "doc_number": str(row['DOC_NUMBER']),
                "line_seq": 1,
                "trans_date": row['TRANS_DATE'].to_pydatetime() if pd.notna(row.get('TRANS_DATE')) else None,
                "financial_year": int(row['financial_year']) if pd.notna(row['financial_year']) else None,
                "financial_month": int(row['financial_month']) if pd.notna(row['financial_month']) else None,
                "financial_quarter": int(row['financial_quarter']) if pd.notna(row['financial_quarter']) else None,
                "customer": {
                    "customer_number": str(row['CUSTOMER_NUMBER']),
                    "region": {
                        "code": str(row['REGION_CODE']) if pd.notna(row.get('REGION_CODE')) else "Unknown",
                        "desc": str(row['REGION_DESC']) if pd.notna(row.get('REGION_DESC')) else "Unknown"
                    },
                    "category_code": int(row['CCAT_CODE']) if pd.notna(row.get('CCAT_CODE')) else 0,
                    "rep": {
                        "code": str(row['REP_CODE']) if pd.notna(row.get('REP_CODE')) else "Unknown",
                        "desc": str(row['REP_DESC']) if pd.notna(row.get('REP_DESC')) else "Unknown"
                    },
                    "credit_limit": float(row['CREDIT_LIMIT']) if pd.notna(row.get('CREDIT_LIMIT')) else 0.0
                },
                "product": {
                    "inventory_code": str(row['INVENTORY_CODE']),
                    "category": {
                        "code": int(row['PRODCAT_CODE']) if pd.notna(row.get('PRODCAT_CODE')) else 0,
                        "desc": str(row['PRODCAT_DESC']) if pd.notna(row.get('PRODCAT_DESC')) else "Unknown",
                        "brand": {
                            "code": int(row['BRAND_CODE']) if pd.notna(row.get('BRAND_CODE')) else 0,
                            "desc": str(row['PRODBRA_DESC']) if pd.notna(row.get('PRODBRA_DESC')) else "Unknown"
                        },
                        "range": {
                            "code": int(row['PRAN_CODE']) if pd.notna(row.get('PRAN_CODE')) else 0,
                            "desc": str(row['PRAN_DESC']) if pd.notna(row.get('PRAN_DESC')) else "Unknown"
                        }
                    },
                    "style": {
                        "gender": str(row['GENDER']) if pd.notna(row.get('GENDER')) else "Unknown",
                        "material": str(row['MATERIAL']) if pd.notna(row.get('MATERIAL')) else "Unknown",
                        "style": str(row['STYLE']) if pd.notna(row.get('STYLE')) else "Unknown"
                    },
                    "last_cost": float(row['LAST_COST']) if pd.notna(row.get('LAST_COST')) else 0.0
                },
                "quantity": int(row['QUANTITY']) if pd.notna(row.get('QUANTITY')) else 0,
                "unit_sell_price": float(row['UNIT_SELL_PRICE']) if pd.notna(row.get('UNIT_SELL_PRICE')) else 0.0,
                "total_line_price": float(row['TOTAL_LINE_PRICE']) if pd.notna(row.get('TOTAL_LINE_PRICE')) else 0.0,
                "transaction_type": {
                    "code": int(row['TRANSTYPE_CODE']) if pd.notna(row.get('TRANSTYPE_CODE')) else 0,
                    "desc": str(row['TRANSTYPE_DESC']) if pd.notna(row.get('TRANSTYPE_DESC')) else "Unknown"
                }
            }
            docs.append(doc)
        except Exception as e:
            logger.warning(f"⚠️  Skipping row due to error: {e}")
            continue

    logger.info(f"✅ Created {len(docs)} MongoDB documents")
    return docs

# ===========================
# MONGODB LOADING
# ===========================

def load_to_mongodb(docs):
    """Insert documents into MongoDB and create indexes."""
    logger.info("☁️  Connecting to MongoDB Atlas...")
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        if docs:
            result = collection.insert_many(docs)
            logger.info(f"✅ Inserted {len(result.inserted_ids)} documents into {DB_NAME}.{COLLECTION_NAME}")
        else:
            logger.warning("⚠️  No documents to insert")
            return

        # Create indexes
        logger.info("🔧 Creating indexes...")
        collection.create_index([("financial_year", 1), ("financial_month", 1)])
        collection.create_index([("customer.region.code", 1)])
        collection.create_index([("product.category.brand.code", 1)])
        collection.create_index([("trans_date", 1)])
        collection.create_index([("doc_number", 1)])

        logger.info("✅ Indexes created successfully")

        # Validation
        total_in_db = collection.count_documents({})
        logger.info(f"🔍 Validation: {total_in_db} documents in database")

    except Exception as e:
        logger.error(f"❌ MongoDB Error: {e}")
        raise

# ===========================
# MAIN EXECUTION
# ===========================

def main():
    logger.info("🚀 Starting ClearVue ETL Process...")

    # Step 1: Load Data
    dfs = load_data()
    if not dfs:
        logger.error("❌ No data loaded. Exiting.")
        return

    # Step 2: Transform Sales Data
    merged_df = transform_sales_data(dfs)
    if merged_df is None or len(merged_df) == 0:
        logger.error("❌ No sales data transformed. Exiting.")
        return

    # Step 3: Create MongoDB Documents
    mongo_docs = create_mongo_documents(merged_df)
    if not mongo_docs:
        logger.error("❌ No MongoDB documents created. Exiting.")
        return

    # Step 4: Load into MongoDB
    load_to_mongodb(mongo_docs)

    logger.info("🎉 ETL Process Completed Successfully!")

if __name__ == "__main__":
    main()