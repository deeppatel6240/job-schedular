import asyncio
import time

import pandas as pd
from fastapi import FastAPI, WebSocket, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi.middleware.cors import CORSMiddleware

import json

app = FastAPI(swagger_ui_parameters={"syntaxHighlight": False})


origins = ["*"]  #
app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["*"],  # You can specify the allowed HTTP methods (e.g., ["POST"])
        allow_headers=["*"],  # You can specify the allowed headers
)

# Initialize the scheduler
scheduler = BackgroundScheduler()
scheduler.start()
input_csv = pd.read_csv("input.csv")
task_data = {}
last_job_id = 0
task_status = {}


# Models
class Task(BaseModel):
    name: str
    status: Optional[str]
    type: str


class Job(BaseModel):
    jobId: Optional[int]
    isScheduled: bool
    name: str
    schedule: str
    lastrun: Optional[datetime]
    list_of_tasks: List[Task]
    status: Optional[str]
    createdTime: Optional[datetime]
    updatedTime: Optional[datetime]


# Custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


# In-memory database (replace with a real database in production)
jobs_db = []
# Retrieve the status of all jobs from the database
job_statuses = []


# API endpoints
@app.post("/jobs/", response_model=Job)
async def create_job(job: Job, background_tasks: BackgroundTasks):
    global last_job_id  # Use the global last_job_id
    job.jobId = last_job_id + 1  # Generate a new jobId
    last_job_id = job.jobId  # Update the last_job_id
    # Set the initial values for 'lastrun', 'status', 'createdTime', and 'updatedTime'
    job.lastrun = datetime.now()
    job.status = "queued"  # Assuming the initial status is 'queued'
    job.createdTime = datetime.now()
    job.updatedTime = datetime.now()

    # Create a new job and add it to the database
    jobs_db.append(job)

    # If the job is scheduled, add a cron job to the scheduler
    if job.isScheduled and job.schedule:
        trigger = CronTrigger.from_crontab(job.schedule)
        background_tasks.add_task(schedule_job, job, trigger)
    return job


@app.delete("/jobs/{job_id}/")
async def delete_job(job_id: int):
    job = find_job_by_id(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    index = 0
    for i in range(0,len(jobs_db)):
        if jobs_db[i].jobId == job_id:
            index = i
            break

    deleted_job = jobs_db.pop(index)
    # You can also add logic to unschedule the job here if it was scheduled

    return {"message": f"Job {job_id} deleted", "job": deleted_job}


@app.put("/jobs/{job_id}/", response_model=Job)
async def update_job(job_id: str, updated_job: Job):
    job_id_int = int(job_id)  # Convert the path parameter to an integer
    job = find_job_by_id(job_id_int)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    index = 0
    for i in range(0, len(jobs_db)):
        if jobs_db[i].jobId == job_id:
            index = i
            break

    # Get the existing job from the database
    existing_job = jobs_db[index]

    # Update the fields that can be modified
    existing_job.name = updated_job.name
    existing_job.isScheduled = updated_job.isScheduled
    existing_job.schedule = updated_job.schedule
    existing_job.list_of_tasks = updated_job.list_of_tasks

    # Update the 'updatedTime' field
    existing_job.updatedTime = datetime.now()

    # If the job is scheduled, update the schedule in the scheduler
    if existing_job.isScheduled and existing_job.schedule:
        trigger = CronTrigger.from_crontab(existing_job.schedule)
        scheduler.reschedule_job(
            job_id,
            trigger=trigger,
        )

    jobs_db[index] = existing_job
    return existing_job


@app.get("/jobs/", response_model=List[Job])
async def list_jobs():
    # Return a list of all jobs
    return jobs_db


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        # Fetch the latest status of all jobs from the database
        job_status = fetch_job_status()  # Implement a function to fetch job status
        await websocket.send_json(job_status)
        await asyncio.sleep(5)  # Send updates every 5 seconds


@app.get("/get-task-status/{dict_key}")
async def get_task_status(dict_key: str):
    return {"dict_key": dict_key, "status": task_status.get(dict_key)}


@app.get("/get-task-cols/{dict_key}")
async def get_task_output_cols(dict_key: str):
    task = task_data.get(dict_key, {})

    input_df = task.get("input_df", pd.DataFrame())  # Corrected access to input_df
    output_df = task.get("output_df", pd.DataFrame())  # Corrected access to output_df

    # Prepare dictionary for input dataframe columns
    input_dict_data = {
        "columns": input_df.columns.tolist(),
        "data_types": input_df.dtypes.apply(lambda x: x.name).to_dict()
    }

    # Prepare dictionary for output dataframe columns
    output_dict_data = {
        "columns": output_df.columns.tolist(),
        "data_types": output_df.dtypes.apply(lambda x: x.name).to_dict()
    }

    # Prepare the result dictionary
    result = {
        "task_id": dict_key,
        "input_df": input_dict_data,
        "output_df": output_dict_data
    }
    return result


@app.get("/get-task-data/{dict_key}")
async def get_task_output(dict_key: str):
    task = task_data.get(dict_key, {})
    input_df = task.get("input_df", pd.DataFrame())  # Corrected access to input_df
    output_df = task.get("output_df", pd.DataFrame())  # Corrected access to output_df

    # Convert the DataFrame to a list of dictionaries with Python data types
    input_dict_data = input_df.to_dict(orient="records")
    output_dict_data = output_df.to_dict(orient="records")

    return {"task_id": dict_key, "input_df": input_dict_data, "output_df": output_dict_data}


def schedule_job(job: Job, trigger: CronTrigger):
    print("scheduled job")
    scheduler.add_job(
        run_tasks,
        args=[job],  # Pass the job and trigger
        trigger=trigger,
        id=str(len(jobs_db) - 1)
    )
    return {"message": "Task Scheduled"}


def run_tasks(job: Job):
    output_df = None
    job.status = 50
    print("running task")
    # Implement scheduling logic based on the job's cron job configuration
    for task in job.list_of_tasks:
        if task.type == 'MISSING_VALUE_HANDLE':
            task.status = 10
            dict_task1 = handle_missing_value(job, task, input_csv)
            output_df = dict_task1["output_df"]
        elif task.type == 'FILTER':
            task.status = 10
            dict_task2 = filter_data(job, task, pd.DataFrame(output_df))
            output_df = dict_task2["output_df"]
        elif task.type == 'AGGREGATE':
            task.status = 10
            dict_task3 = aggregate_data(job, task, pd.DataFrame(output_df))
            output_df = dict_task3["output_df"]
        else:
            print("Invalid task")

    job.status = 100


def fetch_job_status():

    for job in jobs_db:
        job_status = {
            "jobId": job.jobId,
            "status": job.status,
        }
        for job_status in job_statuses:
            if job_status["jobId"] == job.jobId:
                # Update the status if the job exists
                job_status["status"] = job.status
                return json.dumps(job_statuses, cls=DateTimeEncoder)
        job_statuses.append(job_status)

    # Return the job statuses as a JSON object
    return json.dumps(job_statuses, cls=DateTimeEncoder)


def find_job_by_id(job_id: int):
    for job in jobs_db:
        if job.jobId == job_id:
            return job
    return None


# Task related code
def handle_missing_value(job: Job, task: Task, input_df: pd.DataFrame):
    print("handle missing value")
    # Dict key
    dict_key = str(job.jobId) + '_' + task.name

    # Running
    time.sleep(1)
    task.status = 50
    task_status[dict_key] = task.status

    # Perform the missing value handling task
    output_df = input_df.dropna()  # Replace this with your actual missing value handling logic
    output_df['salary'] = output_df['salary'].multiply(2)
    time.sleep(10)

    # Completed
    task.status = 100
    task_status[dict_key] = task.status

    # Store the input and output DataFrames
    task_data[dict_key] = {'input_df': input_df, 'output_df': output_df}

    input_dict_data = input_df.to_dict(orient="records")
    output_dict_data = output_df.to_dict(orient="records")

    print("handle missing value completed")
    return {"dict_key": str(dict_key), "input_df": input_dict_data, "output_df": output_dict_data}


def filter_data(job: Job, task: Task, input_df: pd.DataFrame):
    # Dict key
    dict_key = str(job.jobId) + '_' + task.name
    if input_df is None:
        return {"error": "Input DataFrame from the previous task is missing."}

    # Perform the filter data task
    output_df = input_df  # Replace this with your actual filtering logic

    # Store the input and output DataFrames
    task_data[dict_key] = {'input_df': input_df, 'output_df': output_df}

    return {"dict_key": dict_key, "input_df": input_df, "output_df": output_df}


def aggregate_data(job: Job, task: Task, input_df: pd.DataFrame):
    # Dict key
    dict_key = str(job.jobId) + '_' + task.name
    if input_df is None:
        return {"error": "Input DataFrame from the previous task is missing."}

    # Perform the aggregate data task
    output_df = input_df  # Replace this with your actual aggregation logic

    # Store the input and output DataFrames
    task_data[dict_key] = {'input_df': input_df, 'output_df': output_df}

    return {"dict_key": dict_key, "input_df": input_df, "output_df": output_df}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)