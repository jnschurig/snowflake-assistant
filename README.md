# Snowflake Assistant

The Snowflake Assistant was designed to help make Snowflake easier to manage.

The Warehouse Tagging Assistant was created to mitigate Snowflake spend by applying tagging 
to warehouses, apply duration limits to warehouse size, and make those processes easy.

To run this application locally, you will need to install Python, and the requirements found 
in `requirements.txt`. As of this writing, Python 3.8 is required because of the Snowpark 
python package.

Once Python is installed, open a command prompt and CD to the repository directory. Then run 
`pip install -r requirements.txt` and it should install all the necessary packages.

Then run `streamlit run warehouse_tagging_assistant.py`.

If running this locally is too difficult, feel free to try out the [Snowflake Tagging Assistant on Streamlit Cloud](https://jnschurig-snowflake-assistan-warehouse-tagging-assistant-k0mmww.streamlitapp.com/).

This application is licensed under the GNU GPL3. Please refer to the included license file 
for questions about what you can and can't do with this software.
