import pandas as pd
import numpy as np
from services import get_engine
import math
import datetime as dt

engine_unf = get_engine(fname='../credentials/.prod_unf')
engine_analytics = get_engine(fname='../credentials/.server_analytics')


def read_order(order_no):

    query_order = f'''
    -- order scheduling data
    -- from all_prices_4.sql
    WITH d AS (
        SELECT 
            CAST(items._Fld1527 AS int) AS Артикул
            , items._Description AS Наименование
            , tech._Description AS Технология
            , CAST(DATEADD([YEAR], -2000, _Period) AS date) AS Дата
            , ROW_NUMBER()  OVER (
                                    PARTITION BY 
    --                                    CAST(items._Fld1527 AS int)  -- артикул
                                        items.[_IDRRef]
                                        , price_types._Description      -- ВидЦены
                                    ORDER BY 
                                        CAST(DATEADD([YEAR], -2000, _Period) AS date) DESC  -- Дата
    --                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                    )
                AS row_num
            , prices._Fld7568 AS Цена
            , price_types._Description AS ВидЦены
        FROM _InfoRg7564 AS prices
            LEFT JOIN _Reference36 AS price_types ON prices._Fld7565RRef = price_types._IDRRef
            LEFT JOIN _Reference76 AS items ON prices._Fld7566RRef = items._IDRRef
            LEFT JOIN _Reference79 as tech ON items._Fld1533RRef = tech._IDRRef 
        WHERE
    --        tech._Description IN ('Отливка', 'Протяжка', 'Отливная тяга резина', 'Отливная тяга пластик', 'Фиброгипс')   -- Технология
            price_types._Description = 'Исполнитель'
    )
    , rates AS (
        SELECT 
            Артикул
            , row_num
            , Наименование
            , Технология
            , Дата
            , ВидЦены
            , Цена
        FROM d
    WHERE row_num = 1
    )
    , specs AS (
        -- complex_specs.sql
        SELECT 
        specs._Description AS Спецификация
        , specs.[_Code] AS specId
        , components_list._LineNo16156 AS specLineNo
        , CAST(parent._Fld1527 AS int) AS артикулРодитель
        , parent._Description AS Родитель
        , parent_tech._Description AS родительТехнология
        , parent_cat._Description AS родительКатегория
        , CAST(component._Fld1527 AS int) AS артикулКомпонент
        , component._Description AS Компонент
        , component_tech._Description AS компонентТехнология
        , components_list._Fld16192 AS количество
        , components_list._Fld16163 AS радиус
        , CAST(components_list._Fld16164 AS int) AS лекало
        , IIF(component_tech._Description IN ('Модельные работы', 'Формовочные работы'), components_list._Fld36159, NULL) AS тариф
        , IIF(component_tech._Description IN ('Модельные работы', 'Формовочные работы'), components_list._Fld36160, NULL) AS цена
        FROM _Reference16144 AS specs
        LEFT JOIN _Reference76 AS parent ON specs._Fld16369RRef = parent._IDRRef
        LEFT JOIN _Reference79 AS parent_tech ON parent._Fld1533RRef = parent_tech._IDRRef
        LEFT JOIN _Reference76 AS parent_cat ON parent._ParentIDRRef = parent_cat._IDRRef
        LEFT JOIN _Reference16144_VT16155 AS components_list ON components_list._Reference16144_IDRRef  = specs._IDRRef
        LEFT JOIN _Reference76 AS component ON components_list._Fld16157_RRRef = component._IDRRef
        LEFT JOIN _Reference79 AS component_tech ON component._Fld1533RRef = component_tech._IDRRef
    )
    -- from order_tables.sql
    SELECT 
        numbers._Fld16427 AS orderNo
        , CAST(DATEADD([YEAR], -2000, orders._Fld16402) AS date) AS calcDate
        , tables._Fld35933 AS stage
        , room._Description AS room
        , CAST(tables._LineNo3639 AS int) AS rowNo
        , complex_specs._Description AS spec
        , complex_specs.[_Code] AS specId
        , specs.specLineNo
        , tech._Description AS tech
        , CAST(items._Fld1527 AS int) AS itemId
        , items._Description AS item
        , units._Description AS unit
        , tables._Fld3644 AS qty
        , CASE
        WHEN rates.Цена IS NULL THEN specs.тариф
        ELSE rates.Цена
        END AS rate
        , CASE
        WHEN rates.Цена IS NULL THEN ROUND(specs.тариф * tables._Fld3644, 2)
        ELSE ROUND(rates.Цена * tables._Fld3644, 2)
        END AS pay
    FROM _Document164_VT3638X1 AS tables
        LEFT JOIN _Reference76 AS items ON items._IDRRef = tables._Fld3640RRef
        LEFT JOIN _Reference64 as units ON items._Fld1529RRef = units._IDRRef
        LEFT JOIN _Reference79 as tech ON items._Fld1533RRef = tech._IDRRef
        LEFT JOIN _Document164X1 AS orders ON tables._Document164_IDRRef = orders._IDRRef
        LEFT JOIN _InfoRg16413 as numbers on numbers._Fld16426RRef = orders._idrref
        LEFT JOIN _Reference16143 AS room ON room._IDRRef = tables._Fld16181RRef
        LEFT JOIN _Reference16144 AS complex_specs ON tables._Fld16182RRef = complex_specs._IDRRef
        LEFT JOIN rates ON rates.Артикул = CAST(items._Fld1527 AS int)    -- Артикул
        LEFT JOIN specs ON specs.specId = complex_specs.[_Code] AND specs.артикулКомпонент = CAST(items._Fld1527 AS int)
    WHERE 
        numbers._Fld16427 LIKE '{order_no}' 
    --    CAST(orders._Posted AS int) = 1
    --    AND items._Description IS NOT NULL
        AND CAST(items._Fld1527 AS int) NOT IN (
            200051,  -- не доставка
            200050, -- просушка
            0,
            200049  -- монтаж
        )     
    ORDER BY tables._LineNo3639  -- номер строки
    '''

    order = pd.read_sql(query_order, engine_unf)

    hr_rate = math.ceil(75_000 / 21 / 8)
    # order['hrs'] = (order.pay / hr_rate).apply(math.ceil)
    shops = {
        "Протяжка": "Протяжка",
        "Отливка": "Отливка",
        "Отливная тяга резина": "Отливка",
        "Отливная тяга пластик": "Отливка",
        "Фиброгипс": "Отливка",
        "Модельные работы": "Модели",
        "Формовочные работы": "Формы"
    }
    order['shop'] = order.tech.map(shops)
    order.shop.fillna('Прочие', inplace=True)

    print('Excluded:')
    print(order[order.shop=='Прочие'][['rowNo', 'item']])

    order = order[order.shop!='Прочие']
    
    # TODO: use power data, not const(2)
    order['pwr'] = order.apply(lambda r: r.rate * 2 if r.shop=='Отливка' else None
                               , axis=1)
    
    return order


def get_calendar():
    query = '''
    SELECT 
        CAST(DATEADD([YEAR], -2000, _Fld6647) AS date) Дата
        --, CAST(_Fld6649RRef AS uniqueidentifier) AS ВидДня
        , CASE
            WHEN CAST(_Fld6649RRef AS uniqueidentifier) IN ('DE79BA8C-92F0-7714-4F63-B787C3B5C81F', '9031DDB1-C61A-DC1C-4A16-97C6C8B0A1BE') THEN 1
            ELSE 0
        END AS workDay
    FROM _InfoRg6645
    WHERE _Fld6648 BETWEEN 2022 AND 2025   -- year
    '''
    calendar = pd.read_sql(query, engine_unf)
    return calendar


def get_schedule(order):
    '''
    Schedule by hour.
    All jobs rounded up to the whole hour.
    Resources available capacity in roubles by hour.
    Job's capacity utilization is in roubles. Job takes capacity up to its capacity utilization.
    The last hour of a job may be fractional. Nevertheless the job's capacity utilization is rounded up to whole hour.
    
    Individual specifications are sorted by spec row number and item production row is moved to the end.
    The jobs withen the specifications scheduled sequentially finish-to-start.
    Th subsequent job starts the next day after the previous one finishes, not the next hour.
    '''
    n_modelers = 3
    n_molders = 3
    n_pullers = 2
    n_casters = 2
    mold_pwr = 2    # DEV then get it from the db
    date0 = dt.date(2022, 9, 30)
    
    idx_col0 = (['Модели'] * n_modelers + ['Формы'] * n_molders + 
               ['Протяжка'] * n_pullers + ['Отливка'] * n_casters)
    idx_col1 = list(range(n_modelers))
    idx_col1.extend(range(n_molders))
    idx_col1.extend(range(n_pullers))
    idx_col1.extend(range(n_casters))
    
    idx_col = [idx_col0, idx_col1]
    idx_col = list(zip(*idx_col))
    idx_col = pd.MultiIndex.from_tuples(idx_col, names=['shop', 'resource'])
    
    # idx_row = pd.MultiIndex.from_product([range(10), range(8)], names=['day', 'hr'])

    schedule = pd.DataFrame(
        75_000 / 21 /8,
        # index=idx_row,
        index=range(200),
        columns=idx_col
    )
    
    log = pd.DataFrame(columns=[
        'rowNo',        # order row number
        'time_slot',    # (day, hr) tuple
        'shop',
        'resource',
        'consumption'   # in roubles
    ])
    
    def find_start(shop: str):
        for start in schedule.index:
            for resource in schedule[shop].columns:
                capacity = schedule.loc[start, (shop, resource)]
                if capacity > 0: break
                if capacity < 0:
                    print("Error", shop, str(resource), start)
            else:
                continue
            break        
        
        return start, resource
    
    for i, job in order.iterrows():
        if job.spec:
            pass
        else:
            if job.shop=='Отливка':
                pay_left = job.pay
                # find 1st available resource
                start, resource = find_start('Отливка')
                        
                # schedule the job
                for d in range(start, schedule.shape[0] + 1):
                    '''
                    6 combinations possible:
                    - capacity > pay_left > pwr
                    - capacity > pwr > pay_left
                    - pay_left > pwr > capacity
                    - pay_left > capacity > pwr
                    - pwr > pay_left > capacity
                    - pwr > capacity > pay_left
                    '''
                    capacity = schedule.loc[d, ('Отливка', resource)]
                    step = min(pay_left, job.pwr, capacity)
                    schedule.at[d, ('Отливка', resource)] = capacity - step
                    pay_left -= step
                    log.loc[log.shape[0]] = [
                        job.rowNo, d, 'Отливка', resource,step
                        ]
                    
                    if pay_left <= 0: break
                    
                    # TODO: add extra rows to the schedule if run out of rows
                    
                
                print(job.rowNo)
            
            elif job.shop=='Протяжка':
                pay_left = job.pay
                
    pass


def schedule():
    order = read_order('2239%')
    calendar = get_calendar()
    get_schedule(order)
    print(f'{order.shape=}')
    print(order.iloc[:5, :6])
    
    
    
if __name__ == '__main__':
    schedule()
    