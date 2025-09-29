
from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from db import get_connection
import io
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from langchain_google_genai import ChatGoogleGenerativeAI
import os
from dataprocessor import sanitize_column_name,generate_table_name,process_csv, infer_sql_type, generate_create_table_query, compute_row_hash, get_table_schema, schemas_match, get_all_tables, get_file_id_for_table, get_run_id_for_file

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
        raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")

@app.post("/upload-with-details/")
async def upload_file_with_details(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        date_columns = [col for col in df.columns if df[col].astype(str).str.match(r'\d{2}-\d{2}-\d{4}').any()]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce')
        
        # Infer data types
        inferred_types = [(col, infer_sql_type(df[col].dtype, df, col)) for col in df.columns]
        
        # Sample up to 10 random rows
        sample_size = min(10, len(df))
        sample_data = df.sample(n=sample_size, random_state=42) if sample_size < len(df) else df
        sample_row_numbers = sample_data.index.tolist()
        
        # Check for matching schema
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
        
        # Generate schema if new table is needed
        create_table_query = None
        if not target_table:
            target_table = generate_table_name(file.filename)
            create_table_query = generate_create_table_query(target_table, df, llm)
        
        # Process CSV
        result = process_csv(content, file.filename, llm)
        
        # Get inserted data sample
        df['row_hash'] = df.apply(compute_row_hash, axis=1)
        df.columns = [sanitize_column_name(col) for col in df.columns]
        cursor = conn.cursor()
        cursor.execute(f"SELECT row_hash FROM {result['table_name']}")
        existing_hashes = {row['row_hash'] for row in cursor.fetchall()}
        inserted_rows = df[df['row_hash'].isin(existing_hashes)].head(5).to_dict(orient='records')
        cursor.close()
        conn.close()
        
        return {
            "message": f"File uploaded successfully. Data inserted into table '{result['table_name']}' with {result['row_count']} rows.",
            "file_id": result['file_id'],
            "batch_id": result['batch_id'],
            "run_id": result['run_id'],
            "inferred_types": inferred_types,
            "sample_row_numbers": sample_row_numbers,
            "schema_query": create_table_query,
            "inserted_rows": inserted_rows,
            "metadata": {
                "table_name": result['table_name'],
                "file_id": result['file_id'],
                "batch_id": result['batch_id'],
                "run_id": result['run_id'],
                "row_count": result['row_count']
            }
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")

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
