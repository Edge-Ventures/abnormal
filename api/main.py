import os
import time
import threading
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
import uvicorn

app = FastAPI()

stop_daemon = False

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
TABLE_NAME = os.environ.get("DB_TABLE_NAME")

@app.get("/analyze")
async def analyze_data():
    print("ANALYZE THE DATA")
    try:
        # Create SQLAlchemy engine
        engine = create_engine(DB_CONNECTION_STRING)
        return [{"results": "demon dog"}]
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stop_daemon")
async def stop_daemon():
    print("STOP DAEMON")
    global stop_daemon
    stop_daemon = True
    return {"message": "Daemon stopped"}



@app.get("/status")
async def status():
    global stop_daemon
    return {"status": "running" if not stop_daemon else "stopped"}



@app.get("/run_daemon")
async def run_daemon():
    print("RUN DAEMON")
    global stop_daemon
    stop_daemon = False
    try:
        # Create a thread
        thread = threading.Thread(target=execute_thread)
        thread.start()
        return {"message": "Daemon started"}
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))



def execute_thread():
    global stop_daemon
    while not stop_daemon:
        try:



            print("Thread is running")
            
            # Get the current time
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            
            # Open the file in append mode and write the current time
            with open("./execution_time.txt", "a") as file:
                file.write(current_time + "\n")
            
            print(f"Written to file: {current_time}")
        except Exception as e:
            print(f"Error in thread: {e}")
        
        time.sleep(3)






if __name__ == "__main__":
    
    uvicorn.run(app, host="0.0.0.0", port=8888)