import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from time import sleep
from bs4 import BeautifulSoup
from st_aggrid import AgGrid, ColumnsAutoSizeMode, GridOptionsBuilder
import toml, pymysql, openpyxl, xlsxwriter


with open("config.toml", "r") as toml_file:
    config = toml.load(toml_file)

abc_params = config["database"]

# Set the blue background color using set_page_config
st.set_page_config(
    page_title="ABC Backflow Invoice Management",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

light = """
<style>
    .stApp {
    background-color: lightblue;
    }
</style>
"""
st.markdown(light, unsafe_allow_html=True)

st.markdown(
    """
    <style>
    .custom-button {
        background-color: #001F3F;
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 4px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# Handle missing dates and format existing dates
def format_date(_date):
    if pd.notna(_date):
        return pd.to_datetime(_date).strftime("%m-%d-%Y")  # Customize date format
    return ""


def extract_text(html_string):
    try:
        soup = BeautifulSoup(html_string, "html.parser")
        return soup.get_text()
    except:
        return html_string


def delete_invoices_not_in_staging(params):
    """Delete invoices in ABC_Invoices table that aren't in Staging table."""
    try:
        # Connect to the database
        conn = pymysql.connect(**params)
        cur = conn.cursor()

        # Execute the delete query
        cur.execute(
            """
        DELETE FROM `ABC_Invoices`
        WHERE `Invoice` NOT IN (SELECT `Invoice` FROM `Staging`);
        """
        )

        # Commit changes and provide feedback
        conn.commit()
        deleted_rows = cur.rowcount
        cur.close()

        if deleted_rows > 0:
            st.success(
                f"Deleted {deleted_rows} invoices from 'ABC_Invoices' that were not in 'Staging'!"
            )
        else:
            st.info("No invoices were deleted.")

    except (Exception, pymysql.DatabaseError) as error:
        st.error(f"Database Error: {error}")
    finally:
        if conn is not None:
            conn.close()


def delete_quotes_not_in_staging(params):
    """Delete invoices in Quotes table that aren't in Staging table."""
    try:
        # Connect to the database
        conn = pymysql.connect(**params)
        cur = conn.cursor()

        # Execute the delete query
        cur.execute(
            """
        DELETE FROM `Quotes`
        WHERE `Quote` NOT IN (SELECT `Quote` FROM `Quotes_Staging`);
        """
        )

        # Commit changes and provide feedback
        conn.commit()
        deleted_rows = cur.rowcount
        cur.close()

        if deleted_rows > 0:
            st.success(
                f"Deleted {deleted_rows} invoices from 'Quotes' that were not in 'Quotes_Staging'!"
            )
        else:
            st.info("No quotes were deleted.")

    except (Exception, pymysql.DatabaseError) as error:
        st.error(f"Database Error: {error}")
    finally:
        if conn is not None:
            conn.close()


def insert_new_invoices(params):
    """Insert new invoices from Staging table into ABC_Invoices table."""
    try:
        # Connect to the database
        conn = pymysql.connect(**params)
        cur = conn.cursor()

        # Execute the insert query for new invoices
        cur.execute(
            """INSERT INTO `ABC_Invoices`
        SELECT * FROM `Staging`
        WHERE `Invoice` NOT IN (SELECT `Invoice` FROM `ABC_Invoices`);
        """
        )

        # Commit changes and provide feedback
        conn.commit()
        inserted_rows = cur.rowcount
        cur.close()

        if inserted_rows > 0:
            st.write(
                f"Inserted {inserted_rows} new invoices from 'Staging' into 'ABC_Invoices'!"
            )
        else:
            st.write("No new invoices were inserted.")

    except (Exception, pymysql.DatabaseError) as error:
        st.write(f"Database Error: {error}")
    finally:
        if conn is not None:
            conn.close()


def insert_new_quotes(params):
    """Insert new invoices from Quotes_Staging table into Quotes table."""
    try:
        # Connect to the database
        conn = pymysql.connect(**params)
        cur = conn.cursor()

        # Execute the insert query for new invoices
        cur.execute(
            """INSERT INTO `Quotes`
        SELECT * FROM `Quotes_Staging`
        WHERE `Quote` NOT IN (SELECT `Quote` FROM `Quotes`);
        """
        )

        # Commit changes and provide feedback
        conn.commit()
        inserted_rows = cur.rowcount
        cur.close()

        if inserted_rows > 0:
            st.write(
                f"Inserted {inserted_rows} new quotes from 'Quotes_Staging' into 'Quotes'!"
            )
        else:
            st.write("No new quotes were inserted.")

    except (Exception, pymysql.DatabaseError) as error:
        st.write(f"Database Error: {error}")
    finally:
        if conn is not None:
            conn.close()


def load_df_to_staging(df, database_name):
    """Load a DataFrame to the 'Staging' table in the database."""
    try:
        # Define the SQLAlchemy engine/connection string
        DATABASE_URL = f"mysql+pymysql://{abc_params['user']}:{abc_params['password']}@{abc_params['host']}:{abc_params['port']}/{abc_params['database']}"

        # Create the engine
        engine = create_engine(DATABASE_URL)

        # Use pandas to_sql function to replace the data in 'Staging' table with the new dataframe's data
        df.to_sql(database_name, engine, if_exists="replace", index=False)

        st.success("Dataframe successfully uploaded to 'Staging' table!")

    except Exception as e:
        st.error(f"Error uploading dataframe to database: {e}")


def connect_to_db(params):
    try:
        DATABASE_URL = f"mysql+pymysql://{abc_params['user']}:{abc_params['password']}@{abc_params['host']}:{abc_params['port']}/{abc_params['database']}"
        engine = create_engine(DATABASE_URL)
        return engine
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None


def fetch_all_invoices(params):
    engine = connect_to_db(params)
    if engine:
        try:
            df = pd.read_sql(
                f"SELECT `Invoice` FROM `ABC_Invoices` order by `Due Date` asc", engine
            )
            return df
        except Exception as e:
            st.error(f"Error fetching all invoices: {e}")
            return None


def fetch_all_quotes(params):
    engine = connect_to_db(params)
    if engine:
        try:
            df = pd.read_sql(
                f"SELECT `Quote` FROM `Quotes` order by `Quote` asc", engine
            )
            return df
        except Exception as e:
            st.error(f"Error fetching all quotes: {e}")
            return None


def fetch_all_quotes_data(params):
    engine = connect_to_db(params)
    if engine:
        try:
            df = pd.read_sql(f"SELECT * FROM `Quotes`", engine)
            return df
        except Exception as e:
            st.error(f"Error fetching all data: {e}")
            return None


def fetch_all_data(params):
    engine = connect_to_db(params)
    if engine:
        try:
            df = pd.read_sql(f"SELECT * FROM `ABC_Invoices`", engine)
            return df
        except Exception as e:
            st.error(f"Error fetching all data: {e}")
            return None


def fetch_invoice(invoice_id, params):
    engine = connect_to_db(params)
    if engine:
        try:
            df = pd.read_sql(
                f"SELECT * FROM `ABC_Invoices` WHERE `Invoice`=%s",
                engine,
                params=(invoice_id,),
            )
            if not df.empty:
                data = df.iloc[0]
                if pd.isnull(data["Action Date"]):
                    data_copy = data.copy()
                    data_copy["Action Date"] = date.today() + timedelta(days=90)
                    st.warning(
                        "Action date automatically changed to 3 months from today, updated as needed"
                    )
                    return data_copy
                return data
            else:
                return None
        except Exception as e:
            st.error(f"Error fetching the invoice: {e}")
            return None


def fetch_quote(invoice_id, params):
    engine = connect_to_db(params)
    if engine:
        try:
            df = pd.read_sql(
                f"SELECT * FROM `Quotes` WHERE `Quote`=%s", engine, params=(invoice_id,)
            )
            if not df.empty:
                data = df.iloc[0]
                if pd.isnull(data["Action Date"]):
                    data["Action Date"] = date.today() + timedelta(days=90)
                    st.warning(
                        "Action date automatically changed to 3 months from today, updated as needed"
                    )
                return data
            else:
                return None
        except Exception as e:
            st.error(f"Error fetching the invoice: {e}")
            return None


def update_invoice(invoice_id, note, action_date, _params):
    sql = "UPDATE `ABC_Invoices` SET `Note` = %s, `Action Date` = %s  WHERE `Invoice` = %s"
    conn = None
    updated_rows = 0
    try:
        params = {
            "host": _params["host"],
            "port": 3306,
            "database": _params["database"],
            "user": _params["user"],
            "password": _params["password"],
        }
        conn = pymysql.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (note, action_date, invoice_id))
        updated_rows = cur.rowcount
        conn.commit()
        cur.close()
        if updated_rows > 0:
            st.success("Invoice Updated Successfully!")
        else:
            st.warning("No rows were updated.")
    except (Exception, pymysql.DatabaseError) as error:
        st.error(f"Database Error: {error}")
    finally:
        if conn is not None:
            conn.close()


def update_quote(invoice_id, note, action_date, _params):
    sql = "UPDATE `Quotes` SET `Note` = %s, `Action Date` = %s  WHERE `Quote` = %s"
    conn = None
    updated_rows = 0
    try:
        params = {
            "host": _params["host"],
            "port": 3306,
            "database": _params["database"],
            "user": _params["user"],
            "password": _params["password"],
        }
        conn = pymysql.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (note, action_date, invoice_id))
        updated_rows = cur.rowcount
        conn.commit()
        cur.close()
        if updated_rows > 0:
            st.success("Quote Updated Successfully!")
        else:
            st.warning("No rows were updated.")
    except (Exception, pymysql.DatabaseError) as error:
        st.error(f"Database Error: {error}")
    finally:
        if conn is not None:
            conn.close()


def save_to_excel(data):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        data.to_excel(writer, sheet_name="Sheet1", index=False)
    return output.getvalue()


def app_past_due_invoices():
    st.title("Past Due Invoices")

    uploaded_file = st.file_uploader("**Choose a file**", type=["xlsx"])

    if uploaded_file:
        file_details = Path(uploaded_file.name)

        if file_details.name.startswith("past"):
            try:
                df = pd.read_excel(uploaded_file)
                df.fillna("", inplace=True)
                try:
                    df.insert(loc=2, column="Note", value="")
                    df.insert(loc=3, column="Action Date", value="")
                    df = df.rename(columns={"#": "Invoice"})
                except:
                    pass
                try:
                    # df['Due Date'] = pd.to_datetime(df['Due Date'].copy())
                    df["Due Date"] = pd.to_datetime(
                        df["Due Date"].copy(), format="%d/%m/%Y"
                    )
                except:
                    pass
                df["Action Date"] = pd.to_datetime(
                    df["Action Date"].copy(), format="%d/%m/%Y"
                )
                m = st.markdown(
                    """
                            <style>
                            div.stButton > button:first-child {
                                background-color: #0099ff;
                                color:#ffffff;
                            }
                            div.stButton > button:hover {
                                background-color: #00ff00;
                                color:#ff0000;
                                }
                            </style>""",
                    unsafe_allow_html=True,
                )
                if st.button("Update Database"):
                    load_df_to_staging(df, "Staging")
                    st.success("Uploaded successfully to the database!")
                    sleep(2)
                    delete_invoices_not_in_staging(abc_params)
                    sleep(2)
                    insert_new_invoices(abc_params)
                df["Invoice"] = df["Invoice"].apply(lambda x: "{:.0f}".format(x))
                # st.dataframe(df)  # Moved printing the table after the upload process
                df["Due Date"] = df["Due Date"].apply(format_date)
                df["Action Date"] = df["Action Date"].apply(format_date)
                AgGrid(data=df, columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS)

            except Exception as e:
                st.error(f"Error reading the file: {e}")
        else:
            st.error(
                "Invalid file format! Please upload files with names starting with 'past'."
            )


def app_quotes_management():
    st.title("Quotes Management")

    all_quotes = fetch_all_quotes(abc_params)
    if all_quotes is not None:
        quote_options = all_quotes["Quote"].tolist()

        selected_quote_id = st.selectbox("**Choose a quote**", quote_options)

        data = fetch_quote(selected_quote_id, abc_params)
        if data is not None:
            col1, col2, col3 = st.columns([15, 65, 20])

            with col1:
                styled_box = f"<div style='background-color: white; padding: 5px; border: 2px solid blue; color: blue; display: inline-block;'>{selected_quote_id}</div>"
                st.write(
                    f"<p style='display: inline;'><b>Quote:</b> {styled_box}</p>",
                    unsafe_allow_html=True,
                )

            with col2:
                styled_box = f"<div style='background-color: white; padding: 5px; border: 2px solid blue; color: blue; display: inline-block;'>{data['Name']}</div>"
                st.write(
                    f"<p style='display: inline;'><b>Customer:</b> {styled_box}</p>",
                    unsafe_allow_html=True,
                )

            with col3:
                try:
                    action_date = st.date_input(
                        "**Action Date**", pd.to_datetime(data["Action Date"])
                    )
                except:
                    placeholder_date = date.today() + timedelta(days=90)
                    action_date = st.date_input("**Action Date**", placeholder_date)
                    st.warning(
                        "Action date automatically changed to 3 months from today, updated as needed"
                    )

            note = st.text_area(
                "**Enter a Note - Initials, Date, Note -- Add Each Note on a Separate Line!**",
                data["Note"],
            )

            m = st.markdown(
                """
            <style>
            div.stButton > button:first-child {
                background-color: #0099ff;
                color:#ffffff;
            }
            div.stButton > button:hover {
                background-color: #00ff00;
                color:#ff0000;
                }
            </style>""",
                unsafe_allow_html=True,
            )

            if st.button(f"Update Quote {selected_quote_id} for {data['Name']}"):
                action_date_str = action_date.strftime("%Y-%m-%d")
                update_quote(selected_quote_id, note, action_date_str, abc_params)

        col1, col2 = st.columns([1, 1])
        col1.subheader("Quote Records")

        all_data = fetch_all_quotes_data(abc_params)
        if all_data is not None:
            all_data["Action Date"] = all_data["Action Date"].apply(format_date)
            AgGrid(all_data)
            today_str = date.today().strftime("%m-%d-%y")
            filename = f"quotes_{today_str}.xlsx"
            file_data = save_to_excel(all_data)
            col2.download_button(
                label="**Download**",
                data=file_data,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


def app_quotes_update():
    st.title("Quotes Update")

    uploaded_file = st.file_uploader("**Choose a file**", type=["csv"])

    if uploaded_file:
        file_details = Path(uploaded_file.name)

        if file_details.name.startswith("quote"):
            try:
                df = pd.read_csv(uploaded_file)
                df.fillna("", inplace=True)
                try:
                    df.insert(loc=2, column="Note", value="")
                    df.insert(loc=3, column="Action Date", value="")
                except:
                    pass
                df = df[df["Name"] != "Totals"]
                df["Action Date"] = pd.to_datetime(
                    df["Action Date"].copy(), format="%d/%m/%Y"
                )

                for index, row in df.iterrows():
                    df.at[index, "Name"] = extract_text(row["Name"])
                    df.at[index, "Invoice"] = extract_text(row["Invoice"])
                df.rename(columns={"Invoice": "Quote"}, inplace=True)
                df = df.drop(columns=["Tax Amount"])
                m = st.markdown(
                    """
                            <style>
                            div.stButton > button:first-child {
                                background-color: #0099ff;
                                color:#ffffff;
                            }
                            div.stButton > button:hover {
                                background-color: #00ff00;
                                color:#ff0000;
                                }
                            </style>""",
                    unsafe_allow_html=True,
                )
                if st.button("Update Database"):
                    load_df_to_staging(df, "Quotes_Staging")
                    st.success("Uploaded successfully to the database!")
                    sleep(2)
                    delete_quotes_not_in_staging(abc_params)
                    sleep(2)
                    insert_new_quotes(abc_params)

                df["Action Date"] = df["Action Date"].apply(format_date)
                AgGrid(df)  # Moved printing the table after the upload process

            except Exception as e:
                st.error(f"Error reading the file: {e}")
        else:
            st.error(
                "Invalid file format! Please upload files with names starting with 'quote'."
            )


def app_invoices_management():
    st.title("Invoice Management")

    all_invoices = fetch_all_invoices(abc_params)
    if all_invoices is not None:
        invoice_options = all_invoices["Invoice"].tolist()

        selected_invoice_id = st.selectbox("**Choose an invoice**", invoice_options)

        data = fetch_invoice(selected_invoice_id, abc_params)
        if data is not None:
            col1, col2, col3 = st.columns([15, 65, 20])

            with col1:
                styled_box = f"<div style='background-color: white; padding: 5px; border: 2px solid blue; color: blue; display: inline-block;'>{selected_invoice_id}</div>"
                st.write(
                    f"<p style='display: inline;'><b>Invoice:</b> {styled_box}</p>",
                    unsafe_allow_html=True,
                )

            with col2:
                styled_box = f"<div style='background-color: white; padding: 5px; border: 2px solid blue; color: blue; display: inline-block;'>{data['Customer Name']}</div>"
                st.write(
                    f"<p style='display: inline;'><b>Customer:</b> {styled_box}</p>",
                    unsafe_allow_html=True,
                )

            with col3:
                try:
                    action_date = st.date_input(
                        "**Action Date**", pd.to_datetime(data["Action Date"])
                    )
                except:
                    placeholder_date = date.today() + timedelta(days=90)
                    action_date = st.date_input("Action Date", placeholder_date)
                    st.warning(
                        "Action date automatically changed to 3 months from today, updated as needed"
                    )

            note = st.text_area(
                "**Enter a Note - Initials, Date, Note -- Add Each Note on a Separate Line!**",
                data["Note"],
            )
            m = st.markdown(
                """
                        <style>
                        div.stButton > button:first-child {
                            background-color: #0099ff;
                            color:#ffffff;
                        }
                        div.stButton > button:hover {
                            background-color: #00ff00;
                            color:#ff0000;
                            }
                        </style>""",
                unsafe_allow_html=True,
            )
            if st.button(
                f"Update Invoice {selected_invoice_id} for {data['Customer Name']}"
            ):
                action_date_str = action_date.strftime("%Y-%m-%d")
                update_invoice(selected_invoice_id, note, action_date_str, abc_params)
        else:
            st.warning("No invoice found with that ID")

        col1, col2 = st.columns([1, 1])

        all_data = fetch_all_data(abc_params)
        all_data.sort_values(by="Action Date", ascending=False, inplace=True)
        blank_note_rows = (all_data["Note"].str.strip() == "").sum()
        # col1.subheader(f"Invoice Records ({blank_note_rows} Customers Not Yet Contacted)")
        # Create the subheader with custom formatting
        col1.markdown(
            f"### Invoice Records (<span style='color:red;'>{blank_note_rows} Customers Not Yet Contacted</span>)",
            unsafe_allow_html=True,
        )

        if all_data is not None:
            all_data["Invoice"] = all_data["Invoice"].apply(
                lambda x: "{:.0f}".format(x)
            )
            all_data["Action Date"] = all_data["Action Date"].apply(format_date)
            all_data["Due Date"] = all_data["Due Date"].apply(format_date)
            builder = GridOptionsBuilder.from_dataframe(all_data)
            # Configure the column definitions for the GridOptionsBuilder instance.
            builder.configure_column("Invoice", width=100)
            builder.configure_column("Due Date", width=100)
            builder.configure_column("Note", width=400)
            builder.configure_column("Rows", width=80)
            builder.configure_column("Action Date", width=120)
            builder.configure_column("PO Number", width=150)
            builder.configure_column("Total Amount", width=125)

            # Build the GridOptions object.
            go = builder.build()

            # Create an AgGrid component using the GridOptions object and the all_data variable.
            AgGrid(data=all_data, gridOptions=go)

            today_str = date.today().strftime("%m-%d-%y")
            filename = f"master_invoices_{today_str}.xlsx"
            file_data = save_to_excel(all_data)
            col2.download_button(
                label="**Download**",
                data=file_data,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


apps = {
    "Invoices Management": app_invoices_management,
    "Past Due Invoices": app_past_due_invoices,
    "Quotes Management": app_quotes_management,
    "Quotes Update": app_quotes_update,
}

# Example usage in app.py:
if __name__ == "__main__":
    st.sidebar.title("Navigation")
    choice = st.sidebar.radio("Go to", list(apps.keys()))
    apps[choice]()
