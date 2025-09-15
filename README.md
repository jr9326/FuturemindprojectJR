# FuturemindprojectJakubRdzanek
Follow these steps to set up and run the project:

1.  **Configure Environment**

    First, configure the `.env` file according to your needs.

2.  **Run the ETL Process**

    Navigate to the project's root directory and execute the following command in your terminal. This script will create the `box_office.db` database.
    ```bash
    python3 src/etl_pipeline.py --limit 500
    ```

3.  **Launch the Dashboard**

    After the `box_office.db` database has been successfully created, run the dashboard application with this command:
    ```bash
    python3 src/dashboard_app.py
    ```

---

## ⚠️ Important Note

Please be aware that the collected data may be **incomplete**. This is due to the API's daily limit of 1000 requests.