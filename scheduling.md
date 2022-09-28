# TODO

1. Заменить номера дней на даты
2. Проверить учет количества форм.

# Consider 
1. changing schedule format: tasks — rows, dates — columns
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