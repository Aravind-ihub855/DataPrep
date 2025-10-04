from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Form
from pydantic import BaseModel
from db import get_connection
import io
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from langchain_google_genai import ChatGoogleGenerativeAI
import os
from dataprocessor import (
    sanitize_column_name, generate_table_name, process_csv, infer_sql_type, 
    generate_create_table_query, get_table_schema, schemas_match, get_all_tables, 
    get_file_id_for_file, get_batch_id, get_run_id, compute_data_hash, get_existing_data_hashes
)
import hashlib

app = FastAPI(title="FastAPI + Supabase Postgres")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY not found in environment variables")
llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", google_api_key=GOOGLE_API_KEY)

class User(BaseModel):
    name: str
    email: str

@app.get("/")
def root():
    return {"message": "FastAPI + Supabase backend running"}

@app.post("/users")
def create_user(user: User):
    conn = get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id, name, email",
            (user.name, user.email)
        )
        new_user = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()
        return {"data": new_user, "status": "success"}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/users")
def get_users():
    conn = get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        users = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"data": users, "status": "success"}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...), table_name: str = Form(...), primary_column: str = Form(...), target_table: str = Form(None)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    final_table = target_table if target_table else table_name
    
    try:
        content = await file.read()
        result = process_csv(content, file.filename, llm, final_table, primary_column)
        return {
            "message": f"File uploaded successfully. Data inserted into table '{result['table_name']}' with {result['row_count']} rows.",
            "file_id": result['file_id'],
            "batch_id": result['batch_id'],
            "run_id": result['run_id'],
            "table_name": result['table_name']
        }
    except Exception as e:
        if str(e) == "No new rows to insert (all rows are duplicates).":
            return {
                "message": "No new rows inserted: all rows are duplicates.",
                "file_id": None,
                "batch_id": None,
                "run_id": None,
                "table_name": None
            }
        raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")

@app.post("/infer-types/")
async def infer_types(file: UploadFile = File(...), table_name: str = Form(None), primary_column: str = Form(None)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        if primary_column and primary_column not in df.columns:
            raise HTTPException(status_code=400, detail=f"Primary column '{primary_column}' not found in CSV columns: {df.columns.tolist()}")
        inferred_types = [(sanitize_column_name(col), infer_sql_type(df[col].dtype, df, col)) for col in df.columns]
        return {"inferred_types": inferred_types}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error inferring types: {str(e)}")

@app.post("/sample-rows/")
async def sample_rows(file: UploadFile = File(...), table_name: str = Form(None), primary_column: str = Form(None)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        sample_size = min(10, len(df))
        sample_data = df.sample(n=sample_size, random_state=42) if sample_size < len(df) else df
        sample_row_numbers = sample_data.index.tolist()
        return {"sample_row_numbers": sample_row_numbers, "csv_sample_rows": sample_data.to_dict('records')}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error sampling rows: {str(e)}")

@app.get("/get-table-preview/{table_name}")
async def get_table_preview(table_name: str):
    conn = get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = conn.cursor()
        # Get schema
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position
        """, (table_name,))
        schema = cursor.fetchall()
        schema_dict = {row['column_name']: row['data_type'] for row in schema}
        
        # Get sample rows
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        sample_rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        return {
            "schema": schema_dict,
            "sample_rows": sample_rows,
            "table_name": table_name
        }
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=f"Error fetching table preview: {str(e)}")

@app.post("/generate-schema/")
async def generate_schema(file: UploadFile = File(...), table_name: str = Form(None), primary_column: str = Form(None)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        if not table_name:
            raise HTTPException(status_code=400, detail="Table name is required")
        if primary_column not in df.columns:
            raise HTTPException(status_code=400, detail=f"Primary column '{primary_column}' not found in CSV")
        
        conn = get_connection()
        if not conn:
            raise Exception("Database connection failed")
        
        tables = get_all_tables(conn)
        sanitized_primary = sanitize_column_name(primary_column)
        create_table_query = None
        matching_table = None
        csv_sample_rows = None
        
        if table_name in tables:
            table_schema = get_table_schema(conn, table_name)
            if sanitized_primary not in table_schema:
                raise HTTPException(status_code=400, detail=f"Primary column '{primary_column}' not in existing table '{table_name}'")
            csv_schema = [(sanitize_column_name(col), infer_sql_type(df[col].dtype, df, col)) for col in df.columns]
            if not schemas_match(csv_schema, table_schema):
                raise HTTPException(status_code=400, detail="Schema mismatch with existing table")
            target_table = table_name
        else:
            # Check for schema matches with existing tables
            csv_schema = [(sanitize_column_name(col), infer_sql_type(df[col].dtype, df, col)) for col in df.columns]
            for existing_table in tables:
                existing_schema = get_table_schema(conn, existing_table)
                if sanitized_primary in existing_schema and schemas_match(csv_schema, existing_schema):
                    matching_table = existing_table
                    break
            
            if matching_table:
                # Get preview for matching table
                table_preview = await get_table_preview(matching_table)
                # Get CSV sample
                sample_size = min(5, len(df))
                csv_sample = df.head(sample_size).to_dict('records')
                target_table = table_name  # intended
                return {
                    "schema_query": None,
                    "target_table": target_table,
                    "matching_table": matching_table,
                    "match_confirmed": False,
                    "csv_schema": csv_schema,
                    "csv_sample_rows": csv_sample,
                    "existing_schema": table_preview["schema"],
                    "existing_sample_rows": table_preview["sample_rows"]
                }
            else:
                target_table = table_name
                df_sanitized = df.copy()
                df_sanitized.columns = [sanitize_column_name(col) for col in df.columns]
                create_table_query = generate_create_table_query(target_table, df_sanitized, llm, sanitized_primary)
        
        conn.close()
        return {"schema_query": create_table_query, "target_table": target_table}
    except Exception as e:
        if 'conn' in locals():
            conn.close()
        raise HTTPException(status_code=400, detail=f"Error generating schema: {str(e)}")

@app.post("/check-duplicates/")
async def check_duplicates(file: UploadFile = File(...), table_name: str = Form(None), primary_column: str = Form(None), target_table: str = Form(None)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    final_table = target_table if target_table else table_name
    
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        if not final_table:
            raise HTTPException(status_code=400, detail="Table name is required")
        if primary_column not in df.columns:
            raise HTTPException(status_code=400, detail=f"Primary column '{primary_column}' not found in CSV")
        
        checksum = hashlib.sha256(content).hexdigest()
        conn = get_connection()
        if not conn:
            raise Exception("Database connection failed")
        
        cursor = conn.cursor()
        cursor.execute("SELECT checksum FROM metadata_operations WHERE checksum = %s", (checksum,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return {"message": "No new rows to insert: entire file is a duplicate.", "duplicates": [], "has_duplicates": False}
        cursor.close()
        
        tables = get_all_tables(conn)
        if final_table not in tables:
            conn.close()
            return {"message": "All rows are unique (new table will be created).", "duplicates": [], "has_duplicates": False}
        
        sanitized_primary = sanitize_column_name(primary_column)
        table_schema = get_table_schema(conn, final_table)
        if sanitized_primary not in table_schema:
            conn.close()
            raise HTTPException(status_code=400, detail=f"Primary column '{primary_column}' not in existing table '{final_table}'")
        
        df.columns = [sanitize_column_name(col) for col in df.columns]
        csv_schema = [(col, infer_sql_type(df[col].dtype, df, col)) for col in df.columns]
        if not schemas_match(csv_schema, table_schema):
            conn.close()
            raise HTTPException(status_code=400, detail="Schema mismatch with existing table")
        
        # Check duplicates using primary key
        cursor = conn.cursor()
        cursor.execute(f"SELECT {sanitized_primary} FROM {final_table}")
        existing_primary_values = {row[sanitized_primary] for row in cursor.fetchall()}
        cursor.close()
        
        # Check file-level duplicates via metadata hash
        cursor = conn.cursor()
        cursor.execute("SELECT hash_key FROM metadata_operations WHERE table_name = %s AND hash_key IN (SELECT hash_key FROM metadata_operations WHERE batch_id != (SELECT MAX(batch_id) FROM metadata_operations WHERE table_name = %s))", (final_table, final_table))
        existing_hashes = {row['hash_key'] for row in cursor.fetchall()}
        cursor.close()
        
        # Get duplicate rows based on primary key
        df['primary_key'] = df[sanitized_primary]
        duplicate_rows = df[df['primary_key'].isin(existing_primary_values)].head(5).to_dict('records')
        num_duplicates = len(df[df['primary_key'].isin(existing_primary_values)])
        
        conn.close()
        
        if num_duplicates == 0:
            return {"message": "All rows are unique.", "duplicates": [], "has_duplicates": False}
        else:
            return {
                "message": f"Found {num_duplicates} duplicate rows based on primary key.",
                "duplicates": duplicate_rows,
                "has_duplicates": True
            }
    except Exception as e:
        if 'conn' in locals():
            conn.close()
        raise HTTPException(status_code=400, detail=f"Error checking duplicates: {str(e)}")

@app.post("/confirm-insert/")
async def confirm_insert(
    file: UploadFile = File(...), 
    proceed: str = Form("false"), 
    table_name: str = Form(None), 
    primary_column: str = Form(None),
    target_table: str = Form(None)
):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    final_table = target_table if target_table else table_name
    proceed_bool = proceed.lower() == "true"
    if not proceed_bool:
        return {
            "message": "Insertion canceled by user.", 
            "file_id": None, 
            "batch_id": None, 
            "run_id": None, 
            "table_name": None,
            "row_count": 0
        }
    
    try:
        content = await file.read()
        result = process_csv(content, file.filename, llm, final_table, primary_column)
        return {
            "message": f"File uploaded successfully. Data inserted into table '{result['table_name']}' with {result['row_count']} rows.",
            "file_id": result['file_id'],
            "batch_id": result['batch_id'],
            "run_id": result['run_id'],
            "table_name": result['table_name'],
            "row_count": result['row_count']
        }
    except Exception as e:
        if str(e) == "No new rows to insert (all rows are duplicates).":
            return {
                "message": "No new rows inserted: all rows are duplicates.",
                "file_id": None,
                "batch_id": None,
                "run_id": None,
                "table_name": None,
                "row_count": 0
            }
        raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")

@app.get("/get-batch-file-ids/")
async def get_batch_file_ids():
    conn = get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT batch_id, file_id, file_name FROM metadata_operations WHERE batch_id IS NOT NULL AND file_id IS NOT NULL")
        ids = [{"batch_id": row['batch_id'], "file_id": row['file_id'], "file_name": row['file_name']} for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return {"batch_file_ids": ids}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=f"Error fetching batch and file IDs: {str(e)}")

@app.get("/preview-batch-data/{batch_id}")
async def preview_batch_data(batch_id: int, file_id: int = Query(...)):
    conn = get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM metadata_operations WHERE batch_id = %s AND file_id = %s LIMIT 1", (batch_id, file_id))
        result = cursor.fetchone()
        if not result:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="No data found for this batch_id and file_id")
        
        table_name = result['table_name']
        cursor.execute(f"SELECT * FROM {table_name} WHERE batch_id = %s AND file_id = %s", (batch_id, file_id))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"table_name": table_name, "rows": rows}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=f"Error fetching batch data: {str(e)}")

@app.delete("/delete-batch-data/{batch_id}")
async def delete_batch_data(batch_id: int):
    conn = get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM metadata_operations WHERE batch_id = %s LIMIT 1", (batch_id,))
        result = cursor.fetchone()
        if not result:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="No data found for this batch_id")
        
        table_name = result['table_name']
        cursor.execute(f"DELETE FROM {table_name} WHERE batch_id = %s", (batch_id,))
        cursor.execute("DELETE FROM metadata_operations WHERE batch_id = %s", (batch_id,))
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": f"Data for batch_id {batch_id} deleted successfully from table {table_name}."}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=f"Error deleting batch data: {str(e)}")

@app.on_event("startup")
async def create_metadata_table():
    conn = get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    query = """
    CREATE TABLE IF NOT EXISTS metadata_operations (
        meta_id SERIAL PRIMARY KEY,
        table_name VARCHAR(100),
        file_id INTEGER,
        file_name VARCHAR(255),
        batch_id INTEGER,
        run_id INTEGER,
        operation_type VARCHAR(50),
        checksum VARCHAR(64),
        hash_key VARCHAR(64),
        row_count INTEGER,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Failed to create metadata table: {str(e)}")