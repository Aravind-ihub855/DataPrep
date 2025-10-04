import pandas as pd
import hashlib
from datetime import datetime
import re
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import PromptTemplate
from db import get_connection
import io

def infer_sql_type(dtype, sample_data, col_name):
    """Map pandas dtype to SQL type, refined with sample data."""
    if pd.api.types.is_integer_dtype(dtype):
        max_val = sample_data[col_name].abs().max()
        if pd.isna(max_val) or max_val <= 2147483647:
            return "INTEGER"
        else:
            return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DECIMAL(10,2)"
    elif any(isinstance(x, str) and re.match(r'\d{2}-\d{2}-\d{4}', str(x)) for x in sample_data[col_name].dropna()):
        return "VARCHAR"
    else:
        return "TEXT"

def sanitize_column_name(name):
    """Sanitize column names for SQL compatibility."""
    name = re.sub(r'[^a-zA-Z0-9_]', '_', str(name)).lower()
    if name[0].isdigit():
        name = f"col_{name}"
    return name

def generate_table_name(file_name: str) -> str:
    """Generate a clean table name from file name."""
    base_name = file_name.split('.')[0]
    base_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name).lower()
    base_name = re.sub(r'_+', '_', base_name)
    base_name = base_name.strip('_')
    return base_name

def get_table_schema(conn, table_name):
    """Retrieve schema of an existing table from Supabase."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'public'
        ORDER BY ordinal_position
    """, (table_name,))
    schema = cursor.fetchall()
    cursor.close()
    return {row['column_name']: row['data_type'] for row in schema}

def get_all_tables(conn):
    """Get list of all tables in the public schema, excluding metadata_operations."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name != 'metadata_operations'
    """)
    tables = [row['table_name'] for row in cursor.fetchall()]
    cursor.close()
    return tables

def schemas_match(csv_schema, table_schema):
    """Check if CSV schema matches table schema, ignoring system columns."""
    system_columns = {'file_id', 'batch_id', 'run_id', 'ingestion_timestamp'}
    table_columns = {k: v for k, v in table_schema.items() if k not in system_columns}
    csv_columns = {col: dtype.lower() for col, dtype in csv_schema}
    if csv_columns.keys() != table_columns.keys():
        return False
    for col in csv_columns:
        csv_type = csv_columns[col]
        table_type = table_columns[col].lower()
        type_mapping = {
            'integer': 'integer',
            'bigint': 'bigint',
            'numeric': 'decimal(10,2)',
            'timestamp without time zone': 'timestamp',
            'text': 'text',
            'character varying': 'varchar'
        }
        if type_mapping.get(table_type, table_type) != csv_type:
            return False
    return True

def generate_create_table_query(table_name, df, llm, primary_column):
    """Generate CREATE TABLE query using LangChain and Gemini, including sample data."""
    columns = [(sanitize_column_name(col), infer_sql_type(df[col].dtype, df, col)) for col in df.columns]
    columns_info = "\n".join([f"{col[0]}: {col[1]}" for col in columns])
    sample_size = min(10, len(df))
    sample_data = df.sample(n=sample_size, random_state=42) if sample_size < len(df) else df
    sample_data_str = sample_data.to_csv(index=False, header=True)

    prompt_template = PromptTemplate(
        input_variables=["table_name", "columns_info", "sample_data", "primary_column"],
        template="""You are an expert SQL query generator. Based on the provided table name, column information, and sample data, generate a PostgreSQL CREATE TABLE query. The table must include:
        - The provided columns with their respective SQL types.
        - Four additional columns: file_id (INTEGER), batch_id (INTEGER), run_id (INTEGER), ingestion_timestamp (TIMESTAMP DEFAULT CURRENT_TIMESTAMP).
        - Use 'IF NOT EXISTS' to avoid errors if the table already exists.
        - Set PRIMARY KEY on the column '{primary_column}'.
        - Analyze the sample data to ensure the SQL types are appropriate for the actual data values.
        - For date columns, assume they are in DD-MM-YYYY format and map to VARCHAR, not DATE or TIMESTAMP.
        - For integer columns, use BIGINT if any value in the sample data exceeds 2147483647 in absolute value; otherwise, use INTEGER.
        Table name: {table_name}
        Columns:
        {columns_info}
        Sample data (CSV format):
        {sample_data}

        Return only the SQL query as a string, nothing else. Avoid any additional text, comments, or code fences."""
    )

    query = llm.invoke(prompt_template.format(
        table_name=table_name,
        columns_info=columns_info,
        sample_data=sample_data_str,
        primary_column=primary_column
    )).content
    return query

def compute_data_hash(row, system_cols):
    """Compute SHA256 hash of data columns, excluding system columns."""
    values = [str(row[col]) for col in row.index if col not in system_cols]
    return hashlib.sha256(''.join(values).encode()).hexdigest()

def get_existing_data_hashes(conn, table_name, system_cols):
    """Compute hashes for existing rows in the table, excluding system columns."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    cursor.close()
    hashes = set()
    for row in rows:
        values = [str(row.get(col, '')) for col in row if col not in system_cols]
        h = hashlib.sha256(''.join(values).encode()).hexdigest()
        hashes.add(h)
    return hashes

def get_file_id_for_file(conn, file_name):
    """Retrieve or assign file_id for a file based on file_name."""
    cursor = conn.cursor()
    cursor.execute("SELECT file_id FROM metadata_operations WHERE file_name = %s LIMIT 1", (file_name,))
    result = cursor.fetchone()
    if result:
        file_id = result['file_id']
    else:
        cursor.execute("SELECT COALESCE(MAX(file_id), 0)+1 AS file_id FROM metadata_operations")
        file_id = cursor.fetchone()['file_id']
    cursor.close()
    return file_id

def get_batch_id(conn):
    """Assign a new batch_id for the current processing batch."""
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(batch_id), 0)+1 AS batch_id FROM metadata_operations")
    batch_id = cursor.fetchone()['batch_id']
    cursor.close()
    return batch_id

def get_run_id(conn):
    """Assign a new run_id for the current processing run."""
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(run_id), 0)+1 AS run_id FROM metadata_operations")
    run_id = cursor.fetchone()['run_id']
    cursor.close()
    return run_id

def process_csv(content, file_name, llm, table_name, primary_column):
    """Process CSV, match schema, create table if needed, and insert data."""
    conn = get_connection()
    if not conn:
        raise Exception("Database connection failed")
    
    try:
        df = pd.read_csv(io.BytesIO(content))
        checksum = hashlib.sha256(content).hexdigest()
        
        if primary_column not in df.columns:
            raise Exception(f"Primary column '{primary_column}' not found in CSV")
        
        sanitized_primary = sanitize_column_name(primary_column)
        df.columns = [sanitize_column_name(col) for col in df.columns]
        
        cursor = conn.cursor()
        cursor.execute("SELECT checksum FROM metadata_operations WHERE checksum = %s", (checksum,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            raise Exception("No new rows to insert (all rows are duplicates).")
        cursor.close()
        
        tables = get_all_tables(conn)
        target_table = table_name
        create_table = False
        if target_table in tables:
            table_schema = get_table_schema(conn, target_table)
            if sanitized_primary not in table_schema:
                raise Exception(f"Primary column '{primary_column}' not in existing table '{target_table}'")
            csv_schema = [(col, infer_sql_type(df[col].dtype, df, col)) for col in df.columns]
            if not schemas_match(csv_schema, table_schema):
                raise Exception("Schema mismatch with existing table")
        else:
            create_table = True
        
        file_id = get_file_id_for_file(conn, file_name)
        batch_id = get_batch_id(conn)
        run_id = get_run_id(conn)
        
        if create_table:
            create_table_query = generate_create_table_query(target_table, df, llm, sanitized_primary)
            cursor = conn.cursor()
            cursor.execute(create_table_query)
            conn.commit()
            cursor.close()
        
        # Add system columns
        df['file_id'] = file_id
        df['batch_id'] = batch_id
        df['run_id'] = run_id
        df['ingestion_timestamp'] = datetime.now()
        
        system_cols = {'file_id', 'batch_id', 'run_id', 'ingestion_timestamp'}
        
        # Check duplicates using primary key
        cursor = conn.cursor()
        cursor.execute(f"SELECT {sanitized_primary} FROM {target_table}")
        existing_primary_values = {row[sanitized_primary] for row in cursor.fetchall()}
        cursor.close()
        
        # Filter new rows based on primary key uniqueness
        df['primary_key'] = df[sanitized_primary]
        new_df = df[~df['primary_key'].isin(existing_primary_values)]
        
        if new_df.empty:
            conn.close()
            raise Exception("No new rows to insert (all rows are duplicates based on primary key).")
        
        # Insert new rows
        columns = new_df.columns.tolist()
        columns = [col for col in columns if col != 'primary_key']  # Remove temp column
        quoted_columns = [f'"{col}"' for col in columns]
        columns_sql = ", ".join(quoted_columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {target_table} ({columns_sql}) VALUES ({placeholders})"
        
        cursor = conn.cursor()
        for _, row in new_df.iterrows():
            row_data = [str(row[col]) if pd.isna(row[col]) else row[col] for col in columns]
            cursor.execute(insert_query, tuple(row_data))
        
        conn.commit()
        
        # Update metadata_operations with hash_key as checksum for file-level tracking
        cursor.execute("""
            INSERT INTO metadata_operations (table_name, file_id, file_name, batch_id, run_id, operation_type, checksum, hash_key, row_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            target_table,
            file_id,
            file_name,
            batch_id,
            run_id,
            'INSERT',
            checksum,
            checksum,  # Use checksum as hash_key for metadata
            len(new_df)
        ))
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return {
            "table_name": target_table,
            "file_id": file_id,
            "batch_id": batch_id,
            "run_id": run_id,
            "row_count": len(new_df)
        }
    
    except Exception as e:
        if conn:
            conn.close()
        raise e