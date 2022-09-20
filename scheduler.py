import pandas as pd
from services import get_engine
import math

engine_unf = get_engine(fname='../credentials/.prod_unf')
engine_analytics = get_engine(fname='../credentials/.server_analytics')

order_no = '22399/1/%'
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
    , tables._LineNo3639 AS rowNo
    , complex_specs._Description AS spec
    , complex_specs.[_Code] AS specId
    , tech._Description AS tech
    , CAST(items._Fld1527 AS int) AS itemId
    , items._Description AS item
    , units._Description AS unit
    , tables._Fld3644 AS quantity
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
    AND CAST(items._Fld1527 AS int) NOT IN (200051)     -- не доставка
ORDER BY tables._LineNo3639  -- номер строки
'''

order = pd.read_sql(query_order, engine_unf)

hr_rate = math.ceil(75_000 / 21 / 8)
order['hrs'] = (order.pay / hr_rate).apply(math.ceil)

print(order.head())