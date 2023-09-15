# TODO

1. ~~Удалить отрисовку~~
2. ~~Chk incomlete scheduling in 22552/1.~~
3. ~~Fix timeout error in Excel.~~
4. ~~MR capacity cap exceeded~~
5. ~~custom manufacturing scheduled early, ahead of MFR~~
6. ~~MR scheduled late~~
7. Generate complete error report at once.
8. Chk err msgs.
9. Проверить учет количества форм.
10. A MFR job shall accelerated when more capacity becomes available.
11. Если рабочих форм индивидуального изделия несколько, то при отливке вне состава спецификации 
    использовать все (сейчас используется только одна форма).
12. Может быть сделать симуляцию на независимых агентах? Т.е. реальную симуляцию.

# Consider 
2. use single pool of shop resources, not a number of separate individual resources, i.e. 3 caster as a single 3x power resource.

# Start
`uvicorn scheduling_server:app --reload` 

`--reload` switch for development only

# VSCode Setup
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

Setup VSCode to debug a container: https://medium.com/@nhduy88/setup-debugger-for-your-fastapi-project-with-vscode-and-docker-compose-bc4f61702b69.  
Additionally to use external volume add this to `docker-compose.debug.yml`:
```
volumes:
  scheduling-vol:
    external: true
```

# Build

From `code` folder run  
`docker build -t sashakang/scheduler .`

# Run container

Locally  
`docker run -itv scheduling-vol:/credentials --rm -p 8000:8000 --name scheduler sashakang/scheduler`

`--name scheduler` option needed to schedule container restart in crontab on Ubuntu.

Alternatively  
`docker run -itv scheduling-vol:/credentials --restart=always -p 8000:8000 --name scheduler sashakang/scheduler`  
to restart container after restarting Docker service.  
This may work better with `docker restart scheduler` cron command.

# Create volume

`docker volume create scheduling-vol`

# Volume location

On Windows:  
`\\wsl$\docker-desktop-data\version-pack-data\community\docker\volumes\scheduling-vol\_data\`


# Attach terminal to a running container

`docker exec -it [container ID] bash`

# Debug

To switch between the databases change `база` field in Excel and edit `db` field 
in `.prod_unf` file.

# Algo

## Multiple molds

Multiple molds used to shorten production timeline or if production size exceeds mold life expectancy.

Possible combinations:
1. Multiple molds, no model multiplications. Manufacture a separate model for each mold.
2. Multiple molds with model multiplication. Do model multiplication for each mold.  
   Modelling itself is single.
   If there are multiple molds with single model and no model multiplication or  
   quantity of model multiplication is not equal to number of molds then raise error.

# Crontab

In Ubuntu terminal  
1. Switch to `root` user: `sudo su`.
2. ~~Edit by `nano crontab`.~~ This does not work, opens another instance of crontab.
3. `cd \etc` and use `crontab -e` instead. It opens vim.

# SSH

172.24.1.210