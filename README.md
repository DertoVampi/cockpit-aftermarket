# cockpit-aftermarket
ETL process for a PowerBI-based cockpit that must show many different data types. First draft uses Python to query for data through API to save .parquet file to be read by .duckdb. The focus is to create a light and shareable process that must be robust to avoid any manual data normalization.
