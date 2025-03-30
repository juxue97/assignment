from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware

from store import SimpleStorageService
from constants import APPHOST, APPPORT
from tools import follower_counter

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

database_memory_store = SimpleStorageService(
    bucket_name="data-process-output-29", prefix="output/").load_user_data()


@app.get("/")
async def health_check():
    try:
        return {"Health_Check_Status": "OK", "Connection": "Alive"}

    except Exception as e:
        raise Exception(f"Error starting http server : {e}")


@app.get("/users/{user_id}")
def get_user_by_id(user_id: str):
    try:
        user_data = database_memory_store  # Load or get data

        if user_id in user_data:
            return {"user_id": user_id, **user_data[user_id]}
        else:
            raise HTTPException(status_code=404, detail="User not found")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail="Error while getting data by id")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=APPHOST, port=APPPORT)
