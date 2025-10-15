import streamlit as st
import pandas as pd
import oracledb
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import seaborn as sns
import matplotlib.pyplot as plt

# Inisialisasi Oracle Instant Client
#oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_28")
#st.write("Secrets available:", st.secrets)


def get_connection():
    secrets = st.secrets["oracle"]
    return oracledb.connect(
        user=secrets["user"],
        password=secrets["password"],
        # host=secrets["host"],
        # port=int(secrets["port"]),
        # service_name=secrets["service_name"]
        dsn=f"{st.secrets['oracle']['host']}:{st.secrets['oracle']['port']}/{st.secrets['oracle']['service_name']}"
        #mode=oracledb.DEFAULT_AUTH
    )

def fetch_top6_04t():
    conn = get_connection()
    cursor = conn.cursor()
    
    query = f"""select column1 no, column2 title, column3 status, column4 detail from tops6_04t"""
    
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return pd.DataFrame(rows, columns=columns)

def detail_job_create_file():
    conn = get_connection()
    cursor = conn.cursor()
    query = """SELECT    CRONJOB_NICKNAME,JOB_EXECUTE_TYPE,
            CRONJOB_HANDLE_BY, CRONJOB_RUN_EVERY,STATUS,
            COUNT(1) TOTAL_JOB
            FROM (
            SELECT    A.CRONJOB_NICKNAME,START_DATE,FINISH_DATE,JOB_EXECUTE_TYPE,CRONJOB_HANDLE_BY, CRONJOB_RUN_EVERY,
                      CASE WHEN START_DATE IS NULL THEN 'NOT RUNNING' 
                           WHEN START_DATE IS NOT NULL AND FINISH_DATE IS NULL THEN 'NOT FINISHED'
                           WHEN START_DATE IS NOT NULL AND FINISH_DATE IS NOT NULL THEN 'FINISHED'
                      END STATUS     
            FROM      CRONJOB_PARADISE_NEW  A,(SELECT * FROM CRONJOB_LOG_NEW WHERE CHAR_PERIOD_DATE = TO_CHAR(SYSDATE-1,'RRRRMMDD')) B
            WHERE     A.CRONJOB_NICKNAME = B.CRONJOB_NICKNAME(+)
            UNION 
            SELECT    A.CRONJOB_NICKNAME,START_DATE,FINISH_DATE,'Undefine' JOB_EXECUTE_TYPE,CRONJOB_HANDLE_BY, CRONJOB_RUN_EVERY,
                      CASE WHEN START_DATE IS NULL THEN 'NOT RUNNING' 
                           WHEN START_DATE IS NOT NULL AND FINISH_DATE IS NULL THEN 'NOT FINISHED'
                           WHEN START_DATE IS NOT NULL AND FINISH_DATE IS NOT NULL THEN 'FINISHED'
                      END STATUS
            FROM      CRONJOB_PARADISE  A,(SELECT * FROM CRONJOB_LOG WHERE CHAR_PERIOD_DATE = TO_CHAR(SYSDATE-1,'RRRRMMDD')) B
            WHERE     A.CRONJOB_NICKNAME = B.CRONJOB_NICKNAME(+)
            ) GROUP BY CRONJOB_NICKNAME,JOB_EXECUTE_TYPE,CRONJOB_HANDLE_BY, CRONJOB_RUN_EVERY,STATUS
            ORDER BY   CRONJOB_NICKNAME"""
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return pd.DataFrame(rows, columns=columns)

def detail_job_send_file():
    conn = get_connection()
    cursor = conn.cursor()
    query = """select rownum no,cronjob_nickname, target_ip,cronjob_date, status_desc 
               from  cronjob_log_sendfile
               where cronjob_nickname in (select cronjob_nickname from cronjob_paradise_new
               where cronjob_run_every = 'monthly')
               and   to_char(cronjob_date,'rrrrmm') = to_char(sysdate,'rrrrmm')"""
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return pd.DataFrame(rows, columns=columns)

# Streamlit UI
st.set_page_config(page_title="Dashboard Daily Routine Ops", layout="wide")
st.title("Dashboard Daily Routine Ops")
if st.button("🔄 Refresh Data"):
    df = fetch_top6_04t()
else:
    df = fetch_top6_04t()


# Setup AgGrid
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_selection('single', use_checkbox=True)
grid_options = gb.build()

# Tampilkan tabel utama
grid_response = AgGrid(
    df,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    height=300,
    fit_columns_on_grid_load=True
)

selected = grid_response['selected_rows']
if isinstance(selected, pd.DataFrame) and not selected.empty:
    selected_row = selected.iloc[0]
    #st.write("Selected row:", selected_row)
    #st.write("Keys:", list(selected_row.keys()))
    if selected_row['NO'] == '2' and selected_row['DETAIL'] == 'Y':
        st.subheader(f"Detail {selected_row['TITLE']}")
        st.dataframe(detail_job_create_file())
        st.subheader("📊 Statistik Status Job Create File")
        
        df_detail = detail_job_create_file()
        status_summary = df_detail.groupby("STATUS")["TOTAL_JOB"].sum().reset_index()
        fig, ax = plt.subplots(figsize=(6, 4))
        sns.barplot(data=status_summary, x="STATUS", y="TOTAL_JOB", ax=ax)
        plt.title("Distribusi Status Job")
        plt.xlabel("Status")
        plt.ylabel("Total Job")
        
        st.pyplot(fig)

    elif selected_row['NO'] == '3' and selected_row['DETAIL'] == 'Y':
        st.subheader(f"Detail {selected_row['TITLE']}")
        st.dataframe(detail_job_send_file())
    elif selected_row['NO'] == '1' :
        st.subheader(f"Detail {selected_row['TITLE']}")
        st.warning(f"{selected_row['TITLE']} memang tidak perlu detail")
    else:
        st.error(f"Detail Not Found")