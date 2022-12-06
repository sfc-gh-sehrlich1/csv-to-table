import streamlit as st
from io import StringIO
import pandas as pd
from snowflake.snowpark import Session, Row
from snowflake.snowpark.functions import call_udf, col
from snowflake.snowpark import functions as f
import json

# Display Content #

with open('style.css') as f:
    st.markdown(f'<style>{f.read()}<style>', unsafe_allow_html=True)

st.title("Welcome, upload a csv to create a table in your snowflake account")

# Establish the Connection #

account = st.text_input(
        "Provide an account",
        help = "locator.deployment (e.g. qwe55670.us-east-1)"
    )

user = st.text_input(
        "Provide a username"
    )

password = st.text_input(
        "Provide a password",
        type = "password"
    )

database = st.text_input(
        "Provide a database"
    )

schema = st.text_input(
        "Provide a schema"
    )

warehosue = st.text_input(
        "Provide a warehosue"
    )

role = st.text_input(
        "Provide a role"
    )

result_1 = st.button("Create Connection")

if result_1:

    
    if not account:
        with open('connection.json') as fl:
            connection_parameters = json.load(fl)  
        session = Session.builder.configs(connection_parameters).create()
        st.session_state["session"] = session
        print(st.session_state)
    else:
        connection_json = {
        "account": account,
        "user": user,
        "password": password,
        "role": role,
        "database": database,
        "schema": schema,
        "warehouse": warehosue
    }
        session = Session.builder.configs(connection_json).create()
        print(session)
        st.session_state["session"] = session

# Initialize the Data # 

# Get Stage Info
# session.sql("show stages;").show()
# dropdown_df = session.sql("select $2 from table(RESULT_SCAN(LAST_QUERY_ID()));").to_pandas()

#session.table("\"streamlit_to_table1\"").select(col("LOCATOR")).to_pandas()



# Provision of Table Name
table_name_input = st.text_input(
        "Provide a Table Name 👇"
    )

uploaded_file = st.file_uploader("Choose a file.  NOTE: Must be a csv file")
if uploaded_file is not None:
    session = st.session_state["session"]
    dataframe = pd.read_csv(uploaded_file)
    session.write_pandas(dataframe, table_name=table_name_input, auto_create_table=True, overwrite=True)
    

    # Can be used wherever a "file-like" object is accepted:
    st.write(dataframe[0:1])
    st.write("Your table should be available now")
    uploaded_file = None