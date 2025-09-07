

# 📌 Project Pipeline Overview

### ✅ What are we doing?

1. **Data Conversion & Migration**

   * Convert data from **relational (Excel format)** into **Object format (JSON)**.
   * Push this data into **MongoDB**.

2. **Data Visualization**

   * Create **dashboards in Power BI**.

3. **Data Analysis**

   * Show **aggregations** (e.g., financial years, customer analysis).

4. **Implement Kafka (Real-Time Streaming)**

   * **Why Kafka?**

     1. To simulate **real-time changes** — e.g., when a new payment is initiated (from API or terminal), it must be pushed to MongoDB.
     2. Updates should **automatically reflect in Power BI** (via scheduled refresh or streaming dataset).

5. **Documentation**

   * Write a **report** including all pipeline details (referencing the **Phase 2 Rubric**).

6. **Video Demonstration**

   * Record a walkthrough of the pipeline.

7. **Submission**

   * Package everything for submission. 😊

---

## 📆 Milestones (as of Sep 7, 2025)

### ✅ 1. Data Migration to MongoDB

* Data successfully pushed into **MongoDB**.

**How to run the migration script:**

```bash
# 1. Create a virtual environment
python -m venv env                          

# 2. Activate the virtual environment
./env/scripts/activate                      # On Windows
source env/bin/activate                     # On Mac/Linux

# 3. Install dependencies
pip install pandas pymongo openpyxl xlrd    

# 4. Run the migration script
python migrate_excel_data_to_db.py
```

