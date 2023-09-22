'''
Schedules order manufacturing process. 

Runs FastAPI server.
Called from Excel.

Tutorial: https://realpython.com/fastapi-python-web-apis/#create-a-first-api
'''
import os
from datetime import datetime as dt
import uvicorn
from fastapi import FastAPI
from scheduler import schedule

app = FastAPI(debug=True)

VERSION = '0.9.1'

@app.get("/")
async def root():
    ''' Testing block '''
    # for testing use http://localhost:8000/
    app_message = os.environ.get("APP_MESSAGE", "The scheduler is up")

    return {"message": app_message}


@app.get('/schedule_production/{order_no}')
def schedule_production(
    order_no: str,
    start: str,
    ts: str,
    night: bool,
    modelers: int,
    molders: int,
    casters: int,
    pullers: int
):
    ''' Calls scheduling procedure '''

    order_no = order_no.replace('-', '/') 
    print(f'Calling {order_no=}, {start=}, {ts=}')
    print(f'Server version {VERSION}')
    schedule(
        order_no,
        start,
        ts,
        night,
        modelers,
        molders,
        casters,
        pullers
    )
    current_datetime = dt.today()
    print(f'***Scheduled {current_datetime.strftime("%Y-%m-%d %H:%M:%S")}***')
    return 'Scheduled successfully'


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
