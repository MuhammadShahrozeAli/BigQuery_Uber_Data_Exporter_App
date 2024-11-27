import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import date
from io import BytesIO

# Initialize the BigQuery client
client = bigquery.Client()
st.session_state.reports = {}

# Define datasets and tables
datasets_and_tables = {
    "bpo_datasharing": [
        "Bliss_Msg_Agent_Compliance_Daily",
        "Bliss_Phone_Ticket_CSAT_2_Daily",
        "Bliss_Phone_Ticket_KPIs_Half_Hour_Daily",
        "Bliss_Msg_Ticket_Solved_Weekly",
        "Bliss_Msg_Agent_EMEA_Triage_KPIs_Weekly",
        "Bliss_Msg_Agent_KPIs_Weekly",
        "Bliss_Msg_Chat_Ticket_CSAT_2_Weekly",
        "Bliss_Phone_Ticket_CSAT_2_Weekly",
        "Bliss_Chat_Ticket_Solved_Weekly"
        # Add more tables
    ],
    "documents": [
        "Global_Identity_Document_Agent_Active_Audits_Daily",
        "Global_Identity_Document_Agent_Active_Audits_Weekly"
        # Add more tables
    ],
    "salesforce":
        [
            "Salesforce_Eats_Email_Agent_KPIs_Weekly"
        ],
    "agent_status":
    [
        "Bliss_Agent_Status_Daily"
    ]
    # Add more datasets
}

def main_page():
    # Streamlit UI
    st.title("BigQuery Uber Data Exporter")
    st.sidebar.header("Configuration")

    # Select dataset and tables
    dataset_id = st.selectbox("Select Dataset", list(datasets_and_tables.keys()))
    tables = st.multiselect("Select Tables", datasets_and_tables[dataset_id])

    # Start and End date selection
    st.sidebar.header("Select Date Range")
    start_date = st.date_input("Start Date", date.today())  # Default to today
    end_date = st.date_input("End Date", date.today())  # Default to today

    # Validation
    if start_date > end_date:
        st.sidebar.error("Start Date cannot be after End Date.")

    # Export data button
    if st.button("Generate Report"):
        if not tables:
            st.error("Please select at least one table.")
        else:
            for table in tables:
                query = f"""
                SELECT *
                FROM `{client.project}.{dataset_id}.{table}`(@start_date, @end_date)
                """

                # Configure query parameters
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("start_date", "STRING", start_date.strftime('%Y-%m-%d')),
                        bigquery.ScalarQueryParameter("end_date", "STRING", end_date.strftime('%Y-%m-%d')),
                    ]
                )

                try:
                    with st.spinner('Loading...'):
                        # Execute the query
                        query_job = client.query(query, job_config=job_config)
                        result = query_job.result()

                        # Convert to DataFrame
                        df = result.to_dataframe()

                        # Save data in session state for download
                        report_key = f"MIN_LAH-{table}_{start_date}"
                        st.session_state.reports[report_key] = df
                    st.success(f"Data for `{table}` from `{start_date}` to `{end_date}` added to reports.")

                except Exception as e:
                    st.error(f"Error processing table `{table}`: {e}")
                
                finally:
                    tables = None

    # Display download buttons for each report
    st.header("Download Reports")
    if st.session_state.reports:
        for report_key, df in st.session_state.reports.items():
            # Convert DataFrame to CSV bytes
            csv_data = df.to_csv(index=False).encode('utf-8')

            # Display download button
            st.download_button(
                label=f"Download {report_key}",
                data=csv_data,
                file_name=f"{report_key}.csv",
                mime="text/csv"
            )
    else:
        st.write("No reports available. Please generate reports first.")

if __name__ == '__main__':
    main_page()