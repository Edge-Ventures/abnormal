# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, MetaData, Table
from autoviz.AutoViz_Class import AutoViz_Class
import pandas as pd
import json
import os


from dotenv import load_dotenv
from sqlalchemy import func
load_dotenv()
# Access the environment variables
DB_USER = os.getenv('DB_USER')
DB_PASS =  os.getenv('DB_PASS')
DB_SERVER = os.getenv('DB_SERVER')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_TABLE_NAME = "debate"

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_SERVER}:{DB_PORT}/{DB_NAME}"


app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# You'll need to set these environment variables
DB_CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING")

DB_CONNECTION_STRING = DATABASE_URL
TABLE_NAME = DB_TABLE_NAME

@app.get("/analyze")
async def analyze_data():
    print("ANALYSE THE   DATA")
    try:
        # Create SQLAlchemy engine
        engine = create_engine(DB_CONNECTION_STRING)
        
        # Read the table into a pandas DataFrame
        df = pd.read_sql_table(table_name=TABLE_NAME, con=engine,schema="squabble")
        
        # Perform EDA using AutoViz
        AV = AutoViz_Class()
        analysis_results = AV.AutoViz(
            filename="",
            sep=",",
            depVar="",
            dfte=df,
            header=0,
            verbose=0,
            lowess=False,
            chart_format="svg",
            max_rows_analyzed=150000,
            max_cols_analyzed=30,
        )
        
        print(analysis_results)
        # Convert the results to JSON
        # json_results = json.dumps(analysis_results)
        
        return {"results": "demon dog"}
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8888)