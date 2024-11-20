import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv(".env")

def connect_to_supabase():
    try:
        connection = psycopg2.connect(
            host=os.getenv("SUPABASE_HOST"),
            port=os.getenv("SUPABASE_PORT"),
            user=os.getenv("SUPABASE_USER"),
            password=os.getenv("SUPABASE_PASSWORD"),
            dbname=os.getenv("SUPABASE_DB"),
            sslmode="require"
        )
        print("Connected to Supabase!")
        return connection
    except Exception as e:
        print("Error connecting to Supabase:", e)
        return None
