### What are we doing?
1. Convert data from relational(Excel format) to Object Format (JSON) &
 Push data to mongodb.
2. Create dahsboard on powerbi.

3. Show aggregations - e.g the financial years, customer analysis
### 4. implement kafka
Why?
1. To simulate the realtime changes i.e. when a new payment is intiated, from the API or terminal it must be sent to the database and reflect on the powerBI.
2. The reflection must be automatic (we can schedule an interval refresh of powerbi).

### 5 Write the report including all the pipeline details(Refer to phase 2 Rubric)
### 6 Make a video.
### 7 Submit 😊

### Milestones as of Sep 7, 2025.
## 1. I have pushed the data to mongodb.
To run go to the project root and run 

```
    python -m venv env                          # Start virtual environment
    ./env/scripts/activate                      # Activate virtual environment
    pip install pandas pymongo openpyxl xlrd    # install dependencies.
    python migrate_excel_data_to_db.py

```

