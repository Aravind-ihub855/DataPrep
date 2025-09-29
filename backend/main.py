from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from db import get_connection
import io
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from langchain_google_genai import ChatGoogleGenerativeAI
import os
from dataprocessor import sanitize_column_name, generate_table_name, process_csv, infer_sql_type, generate_create_table_query, compute_row_hash, get_table_schema, schemas_match, get_all_tables, get_file_id_for_table, get_run_id_for_file
import hashlib


app = FastAPI(title="FastAPI + Supabase Postgres")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Gemini with LangChain
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY not found in environment variables")
llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", google_api_key=GOOGLE_API_KEY)

# Pydantic model for user data
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
async def upload_file(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    try:
        content = await file.read()
        result = process_csv(content, file.filename, llm)
        return {
            "message": f"File uploaded successfully. Data inserted into table '{result['table_name']}' with {result['row_count']} rows.",
            "file_id": result['file_id'],
            "batch_id": result['batch_id'],
            "run_id": result['run_id']
        }
    except Exception as e:
        if str(e) == "No new rows to insert (all rows are duplicates).":
            return {
                "message": "No new rows inserted: all rows in the file are duplicates.",
                "file_id": None,
                "batch_id": None,
                "run_id": None
            }
        raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")

@app.post("/infer-types/")
async def infer_types(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        inferred_types = [(col, infer_sql_type(df[col].dtype, df, col)) for col in df.columns]
        return {"inferred_types": inferred_types}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error inferring types: {str(e)}")

@app.post("/sample-rows/")
async def sample_rows(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        sample_size = min(10, len(df))
        sample_data = df.sample(n=sample_size, random_state=42) if sample_size < len(df) else df
        sample_row_numbers = sample_data.index.tolist()
        return {"sample_row_numbers": sample_row_numbers}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error sampling rows: {str(e)}")

@app.post("/generate-schema/")
async def generate_schema(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        conn = get_connection()
        if not conn:
            raise Exception("Database connection failed")
        
        csv_schema = [(sanitize_column_name(col), infer_sql_type(df[col].dtype, df, col)) for col in df.columns]
        tables = get_all_tables(conn)
        target_table = None
        for table in tables:
            table_schema = get_table_schema(conn, table)
            if schemas_match(csv_schema, table_schema):
                target_table = table
                break
        
        create_table_query = None
        if not target_table:
            target_table = generate_table_name(file.filename)
            create_table_query = generate_create_table_query(target_table, df, llm)
        
        conn.close()
        return {"schema_query": create_table_query, "target_table": target_table}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=f"Error generating schema: {str(e)}")

@app.post("/check-duplicates/")
async def check_duplicates(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
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
        
        csv_schema = [(sanitize_column_name(col), infer_sql_type(df[col].dtype, df, col)) for col in df.columns]
        tables = get_all_tables(conn)
        target_table = None
        for table in tables:
            table_schema = get_table_schema(conn, table)
            if schemas_match(csv_schema, table_schema):
                target_table = table
                break
        
        if not target_table:
            cursor.close()
            conn.close()
            return {"message": "All rows are unique (new table will be created).", "duplicates": [], "has_duplicates": False}
        
        df['row_hash'] = df.apply(compute_row_hash, axis=1)
        df.columns = [sanitize_column_name(col) for col in df.columns]
        cursor.execute(f"SELECT row_hash FROM {target_table}")
        existing_hashes = {row['row_hash'] for row in cursor.fetchall()}
        duplicates = df[df['row_hash'].isin(existing_hashes)].head(5).to_dict(orient='records')
        new_rows = df[~df['row_hash'].isin(existing_hashes)]
        
        cursor.close()
        conn.close()
        
        if new_rows.empty:
            return {"message": "No new rows to insert: all rows are duplicates.", "duplicates": duplicates, "has_duplicates": True}
        if duplicates:
            return {"message": f"Found {len(df) - len(new_rows)} duplicate rows.", "duplicates": duplicates, "has_duplicates": True}
        return {"message": "All rows are unique.", "duplicates": [], "has_duplicates": False}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=f"Error checking duplicates: {str(e)}")

@app.post("/confirm-insert/")
async def confirm_insert(file: UploadFile = File(...), proceed: bool = False):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    if not proceed:
        return {"message": "Insertion canceled by user.", "file_id": None, "batch_id": None, "run_id": None, "row_count": 0}
    
    try:
        content = await file.read()
        result = process_csv(content, file.filename, llm)
        return {
            "message": f"File uploaded successfully. Data inserted into table '{result['table_name']}' with {result['row_count']} rows.",
            "file_id": result['file_id'],
            "batch_id": result['batch_id'],
            "run_id": result['run_id'],
            "row_count": result['row_count']
        }
    except Exception as e:
        if str(e) == "No new rows to insert (all rows are duplicates).":
            return {
                "message": "No new rows inserted: all rows in the file are duplicates.",
                "file_id": None,
                "batch_id": None,
                "run_id": None,
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
        cursor.execute("SELECT DISTINCT batch_id, file_id FROM metadata_operations WHERE batch_id IS NOT NULL AND file_id IS NOT NULL")
        ids = [{"batch_id": row['batch_id'], "file_id": row['file_id']} for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return {"batch_file_ids": ids}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=f"Error fetching batch and file IDs: {str(e)}")

@app.get("/preview-batch-data/{batch_id}")
async def preview_batch_data(batch_id: int):
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
        cursor.execute(f"SELECT * FROM {table_name} WHERE batch_id = %s LIMIT 5", (batch_id,))
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
