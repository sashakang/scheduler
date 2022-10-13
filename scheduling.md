# TODO

1. ~~Удалить отрисовку~~
2. ~~Chk incomlete scheduling in 22552/1.~~
3. ~~Fix timeout error in Excel.~~
4. Generate complete error report at once.
5. Chk err msgs.
6. Проверить учет количества форм.

# Consider 
2. use single pool of shop resources, not a number of separate individual resources, i.e. 3 caster as a single 3x power resource.

# Start
`uvicorn scheduling_server:app --reload` 

`--reload` switch for development only

# Setup
`launch.json` for VSCode to debug and run FastAPI/Uvicorn app.
```
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: module",
            "type": "python",
            "request": "launch",
            "module": "uvicorn",
            "args": ["scheduling_server:app","--reload"]
        }        
    ]
}
```

# Build

From `code` folder run  
`docker build -t sashakang/scheduler .`

# Run container

Locally  
`docker run -itv scheduling-vol:/credentials --rm -p 8000:8000 sashakang/scheduler`

# Create volume

`docker volume create scheduling-vol`

# Volume location

On Windows:  
`\\wsl$\docker-desktop-data\version-pack-data\community\docker\volumes\scheduling-vol\_data\`