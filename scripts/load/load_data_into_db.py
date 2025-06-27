import pandas as pd
import sys
import os

sys.path.append('/home/thangtranquoc/sql_projects/ecomerce-sql-analysis')

# Import config
from sql.config_db.db_config import get_connection

def get_latest_file_in_directory(directory,extension):
    """
    Get the latest file in a directory with a specific extension.
    :param directory: A directory to search for files.
    :param extension: File extension to look for.
    :return: Path to the latest file or None if no files were found.
    """
    files = [os.path.join(directory,f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files,key=os.path.getmtime)
    return latest_file

def drop_duplicates_with_row_number(df,partition_col,order_col):
    """
    Drop duplicates row using row_number
    :param: df: Pandas DataFrame.
    :param: partition_col: Column to to group by.
    :param: order_col: Column to order.
    :return: A DataFrame with no duplicates rows.
    """
    df = df.sort_values(by=order_col,ascending=False)
    df['row_number'] = df.groupby(partition_col).cumcount() + 1
    return df[df['row_number'] == 1].drop(columns='row_number')

def insert_data_from_csv(df,table_name,columns,conflict_columns=None):
    """
    Insert data from a CSV file into PostgresSQL.
    :param: df: Pandas DataFrame.
    :param: table_name: Name of the Postgres Table.
    :param columns: List of columns to insert data into.
    :param conflict_column: List of columns to check for conflicts.
    """
    if df.empty:
        print(f'No data inserted into {table_name}')
        return
    
    conn = get_connection()
    cur = conn.cursor()

    placeholders = ', '.join(['%s'] * len(columns))
    
    col_str = ', '.join(columns)

    if conflict_columns:
        conflict_columns_str = ', '.join(conflict_columns)
        query = f"""
                INSERT INTO {table_name} ({col_str})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_columns_str}) DO NOTHING
"""
    else:
        query = f"""
                INSERT INTO {table_name} ({col_str})
                VALUES ({placeholders})
"""
    for col in columns:
        df[col] = df[col].map(lambda x: None if pd.isna(x) else x.item() if hasattr(x, 'item') else x)

    inserted_rows = 0
    for row in df[columns].drop_duplicates().itertuples(index=False):
        cur.execute(query, tuple(row))
        if cur.rowcount > 0:
            inserted_rows += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f' Inserted data into {table_name}: {inserted_rows}')

def load_data_into_db():
    # Get the latest csv file
    extension = '.csv'
    csv_file = get_latest_file_in_directory('/home/thangtranquoc/sql_projects/ecomerce-sql-analysis/data',extension=extension)
    if not csv_file:
        return
    
    # Read file
    df = pd.read_csv(csv_file,dtype={'product_id' : str, 'category_id' : str})
    df['category_id'] = pd.to_numeric(df['category_id'], errors='coerce').astype('Int64')
    df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce').astype('Int64')
    df = df[['event_time','event_type','product_id','category_id','category_code','brand','price','user_id','user_session']].fillna(value=pd.NA)

    # Drop duplicate rows in dim_categories
    category_df = df[['category_id','category_code']].copy()
    category_df = drop_duplicates_with_row_number(category_df,partition_col = 'category_code',order_col = 'category_id')

    # Insert data into tables

    # dim_categories
    insert_data_from_csv(
        df[['category_id','category_code']].copy(),
        'dim_categories',
        ['category_id','category_code'],
        ['category_id']
    )

    # dim_users
    insert_data_from_csv(
        df[['user_id']].copy(),
        'dim_users',
        ['user_id'],
        ['user_id']
    )

    # dim_products
    insert_data_from_csv(
        df[['product_id','category_id','brand','price']].copy(),
        'dim_products',
        ['product_id','category_id','brand','price'],
        ['product_id']
    )

    # fact_sessions
    insert_data_from_csv(
        df[['user_session','user_id']].rename(columns={
            'user_id' : 'session_user_id'
        }).copy(),
        'fact_sessions',
        ['user_session','session_user_id'],
        ['user_session']
    )

    # fact events
    insert_data_from_csv(
        df[['event_time','event_type','user_id','product_id','user_session']].rename(columns={
            'user_id' : 'event_user_id',
            'product_id' : 'event_product_id',
            'user_session' : 'event_user_session'
        }).copy(),
        'fact_events',
        ['event_time','event_type','event_user_id','event_product_id','event_user_session'],
    )

load_data_into_db()