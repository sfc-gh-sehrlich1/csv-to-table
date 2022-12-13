import streamlit as st
from io import StringIO
import pandas as pd
from snowflake.snowpark import Session, Row
from snowflake.snowpark.functions import call_udf, col
from snowflake.snowpark import functions as f
import json
import requests

def login():
    link = 'https://qub36650.us-east-1.snowflakecomputing.com/oauth/authorize?response_type=code&client_id=l%2B%2BCE7r3X%2BF3KjIf9ncaZ4BKim8%3D&redirect_uri=http%3A%2F%2Flocalhost%3A8503%2F'
    st.markdown("Click here to [login](%s)" % link,unsafe_allow_html=True)
    value1 = st.experimental_get_query_params()
    if value1:
        print(value1["code"][0])
        
        data = {'client_id':client_id,
            'client_secret':client_secret,
            'grant_type':grant_type,
            'scope':scope,
            'redirect_uri':redirect_uri,
            'code':code}

        try:
            r = requests.post(url = token_url, data = data)
            access_token = r.text
            print("Got code: %s: returning token: %s" % (code, access_token))
            access_token = json.loads(access_token)['access_token']
            return "<p>Here is your token:</p><p>%s</p>" % access_token

        except requests.exceptions.RequestException as e:
            print("Got exception: %s" % e)
            return "<p>Exception, please contact your administrator: %s</p>" % e

    return "sam"

user = login()

# st.experimental_get_query_params()

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