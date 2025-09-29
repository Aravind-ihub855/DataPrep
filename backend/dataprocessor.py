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
        # Check max absolute value in sample data
        max_val = sample_data[col_name].abs().max()
        if pd.isna(max_val) or max_val <= 2147483647:  # INTEGER max value
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
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(name)).lower()

def generate_table_name(file_name: str) -> str:
    """Generate a clean table name from file name."""
    # Take only base name (remove extension)
    base_name = file_name.split('.')[0]
    # Replace non-alphanumeric with underscore
    base_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name).lower()
    # Replace multiple underscores with single one
    base_name = re.sub(r'_+', '_', base_name)
    # Strip leading/trailing underscores
    base_name = base_name.strip('_')
    return base_name

def get_table_schema(conn, table_name):
    """Retrieve schema of an existing table from Supabase."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY column_name
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
    system_columns = {'id', 'file_id', 'batch_id', 'run_id', 'ingestion_timestamp', 'row_hash'}
    
    # Filter out system columns from table schema
    table_columns = {k: v for k, v in table_schema.items() if k not in system_columns}
    
    # Convert CSV schema to match PostgreSQL types
    csv_columns = {col: dtype.lower() for col, dtype in csv_schema}
    
    # Ensure same columns and types
    if csv_columns.keys() != table_columns.keys():
        return False
    
    for col in csv_columns:
        csv_type = csv_columns[col]
        table_type = table_columns[col].lower()
        # Map PostgreSQL types to our inferred types for comparison
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

def generate_create_table_query(table_name, df, llm):
    """Generate CREATE TABLE query using LangChain and Gemini, including sample data."""
    columns = [(sanitize_column_name(col), infer_sql_type(df[col].dtype, df, col)) for col in df.columns]
    columns_info = "\n".join([f"{col[0]}: {col[1]}" for col in columns])
    
    # Sample up to 10 random rows (or all if fewer than 10)
    sample_size = min(10, len(df))
    sample_data = df.sample(n=sample_size, random_state=42) if sample_size < len(df) else df
    sample_data_str = sample_data.to_csv(index=False, header=True)

    prompt_template = PromptTemplate(
        input_variables=["table_name", "columns_info", "sample_data"],
        template="""
        You are an expert SQL query generator. Based on the provided table name, column information, and sample data, generate a PostgreSQL CREATE TABLE query. The table must include:
        - An auto-incrementing primary key column named 'id' (SERIAL PRIMARY KEY).
        - The provided columns with their respective SQL types.
        - Five additional columns: file_id (INTEGER), batch_id (INTEGER), run_id (INTEGER), ingestion_timestamp (TIMESTAMP), row_hash (TEXT).
        - Use 'IF NOT EXISTS' to avoid errors if the table already exists.
        - Analyze the sample data to ensure the SQL types are appropriate for the actual data values.
        - For date columns, assume they are in DD-MM-YYYY format and map to VARCHAR, not DATE or TIMESTAMP, except for ingestion_timestamp which should remain TIMESTAMP.
        - For integer columns, use BIGINT if any value in the sample data exceeds 2147483647 in absolute value; otherwise, use INTEGER.
        Table name: {table_name}
        Columns:
        {columns_info}
        Sample data (CSV format):
        {sample_data}

        Return only the SQL query as a string, nothing else. Avoid any additional text, comments, or code fences.
        """
    )

    query = llm.invoke(prompt_template.format(
        table_name=table_name,
        columns_info=columns_info,
        sample_data=sample_data_str
    )).content
    return query

def compute_row_hash(row):
    """Compute SHA256 hash of a row, excluding system columns."""
    system_columns = ['file_id', 'batch_id', 'run_id', 'ingestion_timestamp', 'row_hash']
    values = [str(row[col]) for col in row.index if col not in system_columns]
    return hashlib.sha256(''.join(values).encode()).hexdigest()

def get_file_id_for_table(conn, target_table):
    """Retrieve file_id for an existing table from metadata_operations."""
    cursor = conn.cursor()
    cursor.execute("SELECT file_id FROM metadata_operations WHERE table_name = %s LIMIT 1", (target_table,))
    result = cursor.fetchone()
    cursor.close()
    return result['file_id'] if result else None

def get_run_id_for_file(conn, file_name, batch_id, checksum):
    """Get the next run_id for the file based on its checksum."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COALESCE(MAX(run_id), 0) + 1 AS run_id
        FROM metadata_operations
        WHERE checksum = %s
    """, (checksum,))
    run_id = cursor.fetchone()['run_id']
    cursor.close()
    return run_id

def process_csv(content, file_name, llm):
    """Process CSV, match schema, create table if needed, and insert data."""
    conn = get_connection()
    if not conn:
        raise Exception("Database connection failed")
    
    try:
        # Read CSV without date parsing
        df = pd.read_csv(io.BytesIO(content))
        
        # Compute file checksum
        checksum = hashlib.sha256(content).hexdigest()
        
        # Check for duplicate file
        cursor = conn.cursor()
        cursor.execute("SELECT checksum FROM metadata_operations WHERE checksum = %s", (checksum,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            raise Exception("No new rows to insert (all rows are duplicates).")
        
        # Infer CSV schema
        csv_schema = [(sanitize_column_name(col), infer_sql_type(df[col].dtype, df, col)) for col in df.columns]
        
        # Check for matching table
        tables = get_all_tables(conn)
        target_table = None
        for table in tables:
            table_schema = get_table_schema(conn, table)
            if schemas_match(csv_schema, table_schema):
                target_table = table
                break
        
        # Generate IDs
        cursor = conn.cursor()
        if target_table:
            # Reuse file_id for existing table
            file_id = get_file_id_for_table(conn, target_table)
            if not file_id:
                cursor.execute("SELECT COALESCE(MAX(file_id), 0)+1 AS file_id FROM metadata_operations")
                file_id = cursor.fetchone()['file_id']
        else:
            # New table, new file_id
            cursor.execute("SELECT COALESCE(MAX(file_id), 0)+1 AS file_id FROM metadata_operations")
            file_id = cursor.fetchone()['file_id']
        
        # Generate batch_id for this file_id
        cursor.execute("SELECT COALESCE(MAX(batch_id), 0)+1 AS batch_id FROM metadata_operations WHERE file_id = %s", (file_id,))
        batch_id = cursor.fetchone()['batch_id']
        
        # Determine run_id (increment based on checksum)
        run_id = get_run_id_for_file(conn, file_name, batch_id, checksum)
        cursor.close()
        
        # If no matching table, create a new one
        if not target_table:
            target_table = generate_table_name(file_name)
            create_table_query = generate_create_table_query(target_table, df, llm)
            cursor = conn.cursor()
            cursor.execute(create_table_query)
            conn.commit()
            cursor.close()
        
        # Add system columns
        df['file_id'] = file_id
        df['batch_id'] = batch_id
        df['run_id'] = run_id
        df['ingestion_timestamp'] = datetime.now()
        df['row_hash'] = df.apply(compute_row_hash, axis=1)
        df.columns = [sanitize_column_name(col) for col in df.columns]
        
        # Check for duplicate rows
        cursor = conn.cursor()
        cursor.execute(f"SELECT row_hash FROM {target_table}")
        existing_hashes = {row['row_hash'] for row in cursor.fetchall()}
        new_rows = df[~df['row_hash'].isin(existing_hashes)]
        
        if new_rows.empty:
            cursor.close()
            conn.close()
            raise Exception("No new rows to insert (all rows are duplicates).")
        
        # Insert new rows
        columns = new_rows.columns.tolist()
        columns_sql = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {target_table} ({columns_sql}) VALUES ({placeholders})"
        
        cursor = conn.cursor()
        for _, row in new_rows.iterrows():
            # Handle NaN/NaT as strings
            row_data = [str(row[col]) if pd.isna(row[col]) else row[col] for col in columns]
            cursor.execute(insert_query, tuple(row_data))
        
        conn.commit()
        
        # Update metadata_operations
        cursor.execute("""
            INSERT INTO metadata_operations (table_name, file_id, batch_id, run_id, operation_type, checksum, hash_key, row_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            target_table,
            file_id,
            batch_id,
            run_id,
            'INSERT',
            checksum,
            checksum,
            len(new_rows)
        ))
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return {
            "table_name": target_table,
            "file_id": file_id,
            "batch_id": batch_id,
            "run_id": run_id,
            "row_count": len(new_rows)
        }
    
    except Exception as e:
        conn.close()
        raise e
