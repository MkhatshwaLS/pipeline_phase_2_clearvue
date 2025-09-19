import os
import pandas as pd
from pymongo import MongoClient

def import_excel_files(folder_path):
    try:
        # Connect to local MongoDB
        client = MongoClient("mongodb://localhost:27017/")
        db = client["clearvue"]  

        # List all files in the folder
        for file in os.listdir(folder_path):
            if file.endswith(".xlsx") or file.endswith(".xls"):
                file_path = os.path.join(folder_path, file)

                # Read Excel file into DataFrame
                df = pd.read_excel(file_path)

                if not df.empty:
                    # Use filename (without extension) as collection name
                    collection_name = os.path.splitext(file)[0].lower()
                    collection = db[collection_name]

                    # Convert DataFrame to list of dicts and insert
                    data = df.to_dict(orient="records")
                    collection.insert_many(data)

                    print(f" Imported {len(data)} records into '{collection_name}' collection")

        print("All Excel files imported successfully!")

    except Exception as e:
        print("Error importing files:", e)


if __name__ == "__main__":
    folder_path = r"C:\Users\Givenchie\Desktop\NWU 2025\SEMESTER 2\ADV DATABASES-CMPG321\Project\ClearVueBIProj\pipeline_phase_2\pipeline_phase_2_clearvue\exceldata"
    import_excel_files(folder_path)
