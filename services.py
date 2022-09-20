from sqlalchemy import create_engine, engine
import urllib.parse


# TODO: remove `db` from function call, it shall be defined in the file
def get_engine(
    fname: str
) -> engine.base.Engine:
    with open(fname, 'r') as f:
        for line in f:
            # print(f'{line=}')
            if line[0] == '#': continue

            vals = [s.strip() for s in line.split(':')]
            if vals[0] == 'server': 
                server = vals[1]
                continue
            if vals[0] == 'db': 
                db = vals[1]
                continue            
            if vals[0] == 'login': 
                login = vals[1]
                continue
            if vals[0] == 'password':  
                password = urllib.parse.quote_plus(vals[1])
                # print(f'{password=}')
                continue
    
    if not (server and login and password):
        raise ValueError("Server access credentials are not valid")

    # db_str = f'/{db}' if db else ''
    
    # print(f'{password=}')
    
    engine = create_engine(
        f'mssql+pyodbc://{login}:{password}@{server}/{db}'
        f'?driver=ODBC Driver 17 for SQL Server'
        )

    return engine


if __name__ == '__main__':
    eng = get_engine(fname='./.server', db='prod_unf')
    print(type(eng))
    print(eng)