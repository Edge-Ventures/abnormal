from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import pandas_profiling
import sweetviz
import autoviz
from dtale import show
import sqlalchemy

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Adjust this to your React app's URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class DBCredentials(BaseModel):
    host: str
    port: int
    username: str
    password: str
    database: str
    table: str

@app.post("/analyze")
async def analyze_data(credentials: DBCredentials):
    try:
        # Create database connection
        engine = sqlalchemy.create_engine(
            f"postgresql://{credentials.username}:{credentials.password}@{credentials.host}:{credentials.port}/{credentials.database}"
        )
        
        # Read data from the specified table
        query = f"SELECT * FROM {credentials.table}"
        df = pd.read_sql(query, engine)
        
        # Perform analysis using different libraries
        results = {}
        
        # Pandas Profiling
        profile = pandas_profiling.ProfileReport(df)
        results['pandas_profiling'] = profile.to_json()
        
        # Sweetviz
        sweet_report = sweetviz.analyze(df)
        sweet_report.show_html()  # This will save the report as a file
        results['sweetviz'] = 'sweetviz_report.html'  # You'll need to handle file serving
        
        # Autoviz
        autoviz_report = autoviz.AutoViz_Class()
        dft = autoviz_report.AutoViz(
            filename="",
            sep=",",
            depVar="",
            dfte=df,
            header=0,
            verbose=0,
            lowess=False,
            chart_format="html",
            max_rows_analyzed=150000,
            max_cols_analyzed=30,
        )
        results['autoviz'] = 'autoviz_report.html'  # You'll need to handle file serving
        
        # D-Tale
        d = show(df, return_instance=True)
        results['dtale'] = d.main_url()
        
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)