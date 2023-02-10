# from operator import index
import warnings
import math
import sys
from datetime import datetime as dt
import pandas as pd
import sqlalchemy
from services import get_engine

sys.tracebacklimit = 0

engine_unf = get_engine(fname='../credentials/.prod_unf')
engine_analytics = get_engine(fname='../credentials/.server_analytics')

warnings.filterwarnings("ignore")

# TODO: find target salary values for molder and casters
rates = {
    'Модели': 80_000 / 21 / 8,
    'Формы': 80_000 / 21 / 8,
    'Отливка': 80_000 / 21 / 8,
    'Протяжка': 80_000 / 21 / 8
}

query_ind_specs = '''
    -- complex_specs.sql
    SELECT 
    specs._Description AS Спецификация
    , CAST(specs.[_Code] AS int) AS specId
    , CAST(parent._Fld1527 AS int) AS артикулРодитель
    , parent._Description AS Родитель
    , parent_tech._Description AS родительТехнология
    , parent_cat._Description AS родительКатегория
    , units._Description AS parentUnit
    , components_list._LineNo16156 AS specLineNo
    , CAST(component._Fld1527 AS int) AS артикулКомпонент
    , component._Description AS Компонент
    , component_tech._Description AS компонентТехнология
    , components_list._Fld16192 AS количество
--	, components_list._Fld35576 AS количествоДляСайта
    , components_list._Fld16163 AS радиус
    , CAST(components_list._Fld16164 AS int) AS лекало
    , components_list._Fld36357 AS n_casts
    , components_list._Fld36358 / 1000 AS cast_len	
    , IIF(component_tech._Description IN ('Модельные работы', 'Формовочные работы'), components_list._Fld36159, NULL) AS тариф
    , IIF(component_tech._Description IN ('Модельные работы', 'Формовочные работы'), components_list._Fld36160, NULL) AS цена
FROM 
    _Reference16144 AS specs
    LEFT JOIN _Reference76 AS parent ON specs._Fld16369RRef = parent._IDRRef
    LEFT JOIN _Reference79 AS parent_tech ON parent._Fld1533RRef = parent_tech._IDRRef
    LEFT JOIN _Reference76 AS parent_cat ON parent._ParentIDRRef = parent_cat._IDRRef
    LEFT JOIN _Reference16144_VT16155 AS components_list ON components_list._Reference16144_IDRRef  = specs._IDRRef
    LEFT JOIN _Reference76 AS component ON components_list._Fld16157_RRRef = component._IDRRef
    LEFT JOIN _Reference79 AS component_tech ON component._Fld1533RRef = component_tech._IDRRef
    LEFT JOIN _Reference64 as units ON parent._Fld1529RRef = units._IDRRef  
WHERE 
        parent.[_Description] LIKE 'и%'
        AND parent_tech.[_Description] IN ('Отливка', 'Отливная тяга резина', 'Отливная тяга пластик', 'Фиброгипс')
        AND component.[_Description] NOT LIKE 'Отрисовка%'        
'''

ind_specs = pd.read_sql(query_ind_specs, engine_unf)

err_log = pd.DataFrame(columns=['err_msg'])


def read_order(order_no):
    ''' Read customer order from 1C 
    
    Returns:
        pd.DataFrame: order rows.
    '''

    # TODO: remove busts

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
            AND items.[_Description] NOT LIKE '%Паллада%'            
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
        , components_list._Fld36357 AS n_casts
        , components_list._Fld36358 / 1000 AS cast_len
        , IIF(component_tech._Description 
            IN ('Модельные работы', 'Формовочные работы'), 
            components_list._Fld36159, 
            NULL
        ) AS тариф
        , IIF(component_tech._Description 
            IN ('Модельные работы', 'Формовочные работы'), 
            components_list._Fld36160, 
            NULL
        ) AS цена
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
        , IIF(cat._Description IS NULL, subcat._Description, cat._Description) AS cat
        , IIF(cat._Description IS NULL, NULL, subcat._Description) AS subcat
        , CAST(items._Fld1527 AS int) AS itemId
        , items._Description AS item
        , cat._Description AS category
        , units._Description AS unit
        , CAST(items._Fld16170 AS int) AS Высота
        , CAST(items._Fld16171 AS int) AS Ширина
        , CAST(items._Fld17891 AS int) AS Глубина        
        , tables._Fld3644 AS qty
        , specs.n_casts
        , specs.cast_len
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
        LEFT JOIN _Reference76 AS subcat ON subcat._IDRRef = items._ParentIDRRef
        LEFT JOIN _Reference76 AS cat ON cat._IDRRef = subcat._ParentIDRRef   
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
        AND tech._Description 
            NOT IN ('Упаковка', 'Доставка', 'Монтажный шаблон', 'Барельефы')
        AND items.[_Description] NOT LIKE ('%отрисовка%')        
        AND IIF(cat._Description IS NULL, subcat._Description, cat._Description) 
            NOT IN ('Скульптуры')    -- cat
    ORDER BY tables._LineNo3639  -- номер строки
    '''

    order = pd.read_sql(query_order, engine_unf)

    if len(order) == 0:
        raise ValueError('Получен пустой заказ')

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

    if sum(order.shop == 'Прочие'):
        print('Excluded:')
        print(order[order.shop == 'Прочие'][['rowNo', 'item']])

    order = order[order.shop != 'Прочие']

    return order


def fill_power(order, night_shift):
    # TODO: use power data, not const(2)
    # TODO: get mold power for individual products from mold stock, 
    # if unavailable use individual spec
    ''' Get total mold power '''
    query_specs = '''
        -- specs.sql
        SELECT 
            parent._Description AS parent
        --    , CASE
        --        WHEN material.[_EnumOrder] = 0 THEN 'Гипс Г-16'
        --        WHEN material.[_EnumOrder] = 1 THEN 'Полиуретан'
        --        WHEN material.[_EnumOrder] = 2 THEN 'Фиброгипс'
        --        WHEN material.[_EnumOrder] = 3 THEN 'Стеклофибробетон'
        --    END AS material    
            , CAST(parent._Fld1527 AS int) AS АртикулПродукта
            , tech._Description AS tech
            , units._Description AS unit
            , specs._Description AS spec
            , specs.[_Code] AS specCode
            , CAST(specs._Fld16434 AS int) AS Резина
            , CAST(specs._Fld16435 AS int) AS Пластик
            , CAST(specs._Fld16175 AS int) AS ФормаПодИзгиб
            , tool._LineNo2419 AS НомерСтроки
            , items._Description AS Оснастка
            , categories._Description AS toolGroup
            , CAST(items._Fld1527 AS int) AS АртикулОснастки
        --    , serial._Description AS serialNo
            , serial._Fld16389 AS n_casts
            , serial._Fld16388 AS cast_len
        FROM _Reference122 AS specs
            LEFT JOIN _Reference76 AS parent ON specs._OwnerIDRRef = parent._IDRRef
            LEFT JOIN _Reference64 as units ON parent._Fld1529RRef = units._IDRRef    
            LEFT JOIN _Reference79 as tech ON parent._Fld1533RRef = tech._IDRRef
            LEFT JOIN _Reference122_VT2418 AS tool ON tool._Reference122_IDRRef = specs._IDRRef
            LEFT JOIN _Reference76 AS items ON items._IDRRef = tool._Fld2421RRef
            LEFT JOIN _Reference76 AS categories ON categories._IDRRef = items._ParentIDRRef
        --    LEFT JOIN _Reference79 AS toolTech ON items._Fld1533RRef = toolTech._IDRRef
        --    LEFT JOIN _Enum17382 AS material ON material._IDRRef = parent._Fld17384RRef
            LEFT JOIN _Reference14491 AS serial ON serial._OwnerIDRRef = items._IDRRef
        WHERE 
            CAST(specs._Marked AS int) = 0    -- пометка удаления
            AND CAST(specs._Fld35181 AS int) = 0  -- недействителен
            AND tech._Description IN ('Отливка', 'Отливная тяга резина', 'Отливная тяга пластик', 'Фиброгипс')
            AND categories._Description NOT IN ('Модель', 'Шаблоны', 'Сырье и материалы для производства', 'Под изгиб')   -- toolGroup
            AND specs._Description NOT LIKE '%ФИ'
            AND CAST(specs._Fld16175 AS int) = 0    -- ФормаПодИзгиб    
    '''

    mold_serials = pd.read_sql(query_specs, engine_unf)
    mold_serials['pwr'] = mold_serials.apply(lambda r:
                                             r.n_casts if r.unit != 'п.м.'
                                             else r.n_casts * r.cast_len / 1000, axis=1
                                             )
    mold_pwr = mold_serials.groupby(['АртикулПродукта']).agg(sum).pwr

    query_mfr_params = '''
        SELECT *
        FROM mfr_scheduling_params
    '''
    mfr_params = pd.read_sql(query_mfr_params, engine_analytics)

    def get_item_pwr(job):
        try:
            if job.itemId == 200167:    # ФР Изготовление одиночной формы
                # always shall take 1 working day
                return max(0.125, rates[job.shop] / job.rate)

            elif job.shop == 'Модели':
                volume = get_vol_from_spec(job)
                std_days = mfr_params[mfr_params.id ==
                                      job.itemId].std_model_days.values[0]
                days = std_days if volume <= 0.06 else std_days * (1 + volume)
                pwr = job.qty / days / 8
                pwr = min(pwr, rates[job.shop]/job.rate)
                return pwr

            elif job.shop == 'Формы':
                volume = get_vol_from_spec(job)
                # punch itemId
                has_punch = 200181 in order[order.spec ==
                                            job.spec].itemId.values
                model_tariff_id = order[
                    (order.spec == job.spec) &
                    (order.shop == 'Модели') &
                    (order.specLineNo == 2)
                ].itemId.values[0]
                if has_punch:
                    std_days = mfr_params[
                        mfr_params.id == model_tariff_id
                    ].std_mold_days_with_punch.values[0]
                else:
                    std_days = mfr_params[
                        mfr_params.id == model_tariff_id
                    ].std_mold_days_no_punch.values[0]

                days = std_days if volume <= 0.06 else std_days * (1 + volume)
                if job.qty > 1:
                    days = math.ceil(days + job.qty * 1.5)
                pwr = job.qty / days / 8
                pwr = min(pwr, rates[job.shop] / job.rate)
                return pwr

            elif job.shop == 'Протяжка':
                semiperimeter = job.Глубина + (
                    job.Ширина if job.category in [
                        "Полуколонны: ствол гладкий",
                        "Пилястры: ствол гладкий"
                    ]
                    else job.Высота
                )
                if semiperimeter > 170:
                    return 18.4 / 8
                elif semiperimeter > 130:
                    return 36.8 / 8
                elif semiperimeter > 70:
                    return 55.2 / 8
                else:
                    return 73.6 / 8

            else:
                k_night = 2 if night_shift else 1
                casts_per_hr = 0.67 if job.tech == 'Фиброгипс' else 1.25

                if job['item'].startswith('и'):
                    # get custom spec even if it is not in the same order
                    ind_spec = ind_specs[
                        (ind_specs.артикулРодитель == job.itemId) &
                        (ind_specs.компонентТехнология == "Формовочные работы") &
                        (ind_specs.n_casts.notnull() | ind_specs.n_casts > 0) &
                        (ind_specs.артикулКомпонент != 200181)     # не пуансон
                    ]
                    if len(ind_spec) == 0:
                        return 0
                    # if more than 1 row then the last one
                    ind_spec = ind_spec[ind_spec.specLineNo == (
                        ind_spec.specLineNo.max())]
                    ind_spec = ind_spec.iloc[0]

                    # get number of individual molds
                    # works only for the specs in the same order
                    # otherwise it gets too complicated, in this case `n_molds` = 1
                    if job.specId:
                        subset = order[
                            (order.specId == job.specId) &
                            (order.shop == 'Формы')
                        ]
                        if subset.empty:
                            err_log.loc[len(err_log), 'err_msg'] = {
                                'err_msg': f'Ошибка в спецификации {job.spec} в строке {job.rowNo}.'
                            }
                            return 0
                            # raise RuntimeError(
                            #     f'Ошибка в спецификации {job.spec} в строке {job.rowNo}.')
                        subset = subset[subset.specLineNo ==
                                        (subset.specLineNo.max())]
                        subset = subset.iloc[0]
                        if subset.n_casts == 0:
                            subset = order[
                                (order.specId == job.specId) &
                                (order.shop == 'Формы')
                            ]
                            subset[subset.specLineNo == (
                                subset.specLineNo.max()-1)]
                            subset = subset.iloc[0]

                        if subset.n_casts == 0:
                            err_msg = (f'Количество отливок в форме {job["item"]} '
                                       f'в строке {job["rowNo"]} не заполнено')
                            err_log.loc[len(err_log), 'err_msg'] = err_msg
                            print(err_msg)
                            # raise ValueError(err_msg)
                        pwr = subset.qty * subset.n_casts * casts_per_hr
                        if job.unit == 'п.м.':
                            if subset.cast_len == 0:
                                err_msg = (f'Длина погонажной отливки {job["item"]} '
                                           f'в строке {job["rowNo"]} не заполнена')
                                err_log.loc[len(err_log), 'err_msg'] = err_msg
                                print(err_msg)
                                raise ValueError(err_msg)
                            pwr *= subset.cast_len
                        return pwr

                    elif ind_spec.n_casts > 0:
                        # if no spec in the order than assume 1 mold is available
                        pwr = (
                            ind_spec.n_casts
                            * k_night
                            * casts_per_hr
                        )
                        return (
                            pwr if ind_spec.parentUnit != 'п.м.'
                            else pwr * ind_spec.cast_len
                        )
                    else:
                        return 0

                else:
                    try:
                        pwr_units = mold_pwr[job.itemId] * casts_per_hr
                    except IndexError:
                        print(f'No mold power data for {job.item}.')
                    return pwr_units * k_night
        except Exception:
            err_msg = (
                f"Не удалось рассчитать мощность формы {job.spec}/{job['item']} в строке "
                f"{job.rowNo} заказа."
            )

            err_log.loc[len(err_log), 'err_msg'] = err_msg
            print(err_msg)

    def get_vol_from_spec(job):
        cast = order[
            (order.spec == job.spec) &
            (order.shop == 'Отливка')
        ]
        if len(cast) > 1:
            item = job['item']
            err_msg = (
                "Ошибка в спецификации индивидуального изделия: более одной строки "
                f"для производства изделия для {job.spec}/{item} в строках "
                f"{', '.join(cast.rowNo.apply(str))} заказа покупателя."
            )

            err_log.loc[len(err_log), 'err_msg'] = err_msg
            print(err_msg)
            # raise RuntimeError(
            #     "Ошибка в спецификации индивидуального изделия: более одной строки "
            #     f"для производства изделия для {item} в строке {job.rowNo} заказа покупателя."
            # )
            return 0
        cast = cast.iloc[0]
        # TODO: also use diameter
        volume = cast.Высота * cast.Ширина * cast.Глубина / 10**9
        if volume == 0 and cast.unit == 'п.м.':

            def get_cast_len_from_spec(job):
                mold = order[
                    (order.spec == job.spec) &
                    (order.shop == 'Формы') &
                    (order.itemId != 200181)     # не пуансон
                ]
                mold = mold.sort_values('rowNo').iloc[-1]
                return mold.cast_len

            cast_len = get_cast_len_from_spec(job)
            volume = (
                cast.Глубина *  # in mm.
                (cast.Ширина if cast.Ширина > 0 else cast.Высота) *  # in mm.
                cast_len /  # in m.
                10**6
            )
        if not volume:
            err_log.loc[len(err_log), 'err_msg'] = (
                f'Объем изделия должен быть больше 0. Строка {job.rowNo}, {job.spec}/{job["item"]}'
            )
            return 0
            # raise ValueError(f'Volume must be greater than 0.\n{job.__repr__()}')
        return volume

    order['pwr_units'] = order.apply(get_item_pwr, axis=1)
    order['pwr_rub'] = order.pwr_units * order.rate

    return order


def get_calendar(start_date: str = '20220901'):
    ''' Gets working calendar from 1C'''
    query = f'''
    SELECT 
        CAST(DATEADD([YEAR], -2000, _Fld6647) AS date) Дата
    FROM _InfoRg6645
    WHERE 
        _Fld6648 BETWEEN 2022 AND 2025   -- year
        -- workdays only:
        AND CAST(_Fld6649RRef AS uniqueidentifier) IN ('DE79BA8C-92F0-7714-4F63-B787C3B5C81F', '9031DDB1-C61A-DC1C-4A16-97C6C8B0A1BE')
        AND CAST(DATEADD([YEAR], -2000, _Fld6647) AS date) >= '{start_date}'
    '''
    calendar = pd.read_sql(query, engine_unf)
    calendar['date_str'] = calendar.Дата.apply(
        lambda d: d.strftime("%d.%m.%Y"))
    return calendar


# TODO: if multiple molds production shall start after the first one is finished
def get_schedule(
    order,
    timestamp,
    night_shift,
    n_modelers,
    n_molders,
    n_casters,
    n_pullers
):
    '''
    Night shift means mold capacity doubles. Other shops are not affected.

    Schedule by hour.
    All jobs rounded up to the whole hour.
    Resources available capacity in roubles by hour.
    Job's capacity utilization is in roubles. Job takes capacity up to its capacity utilization.
    The last hour of a job may be fractional. Nevertheless the job's capacity utilization is rounded up to whole hour.

    Custom specifications are sorted by spec row number and item production row is moved to the end.
    The jobs withen the specifications scheduled sequentially finish-to-start.
    Th subsequent job starts the next day after the previous one finishes, not the next hour.
    '''

    capacity = {
        'Модели': n_modelers * rates['Модели'],
        'Формы': n_molders * rates['Формы'],
        'Отливка': n_casters * rates['Отливка'] * (2 if night_shift else 1),
        'Протяжка': n_pullers * rates['Протяжка']
    }

    log = pd.DataFrame(columns=[
        'order_no',
        'rowNo',
        'hr',
        'rub_allocated',
        'unit_production'
    ])

    custom_specs = order[
        (order.spec.notnull()) &
        (order.shop.isin(["Модели", "Формы"]))
    ].spec.unique()

    def rearrange_customs():
        '''
        Makes product the last row within custom spec.
        '''
        # TODO: add check for specs shall have consequantial row numbers

        order['index2'] = order.index

        for spec_idx in order.spec_idx.unique():
            rows = order[order.spec_idx == spec_idx].index2

            for row in rows:
                if row == min(rows):
                    if order.shop.loc[row] not in ["Протяжка", "Отливка", "Фиброгипс"]:
                        print(
                            "Warning: production is not the 1st line in the custom spec:",
                            f'row={order.loc[row, "rowNo"]}'
                        )
                    order.at[row, 'index2'] = max(rows)
                else:
                    order.at[row, 'index2'] -= 1

        order.set_index('index2', drop=True, inplace=True)
        order.sort_index(inplace=True)

        return order

    order = rearrange_customs()

    order['scheduled'] = False

    schedule = pd.DataFrame(index=order.index)

    for i, job in order.iterrows():
        if (job.pwr_rub == 0 or job.pwr_units == 0
                or pd.isnull(job.pwr_rub) or pd.isnull(job.pwr_units)):
            err_msg = (
                f'Нулевая мощность: строка {job.rowNo}, '
                f'спецификация {job.spec}, '
                f'{job["item"]}'
            )
            print(err_msg)
            err_log.loc[len(err_log), 'err_msg'] = err_msg
            continue
        # print(f'{i=}')
        if job.scheduled:
            continue      # TODO: excessive?

        def get_start_hr(order, custom_specs, schedule, job):
            hr = 0

            if job.spec in custom_specs:
                # TODO: custom items can be within a spec and also have extra rows without spec
                # TODO: shall be interrelated via resource constraints
                # if last mold within spec and multiple molds
                # if (
                #     job.specLineNo == order[order.spec==job.spec].specLineNo.max()
                #     and job.shop == 'Формы'
                #     and job.qty > 1
                # ):
                #     pass
                # if job.shop not in ['Отливка', 'Фиброгипс'] or job.qty > 1:
                scheduled_hrs = schedule[(
                    order.spec == job.spec).values].astype(bool).sum()
                # last hour scheduled for the spec
                hr = scheduled_hrs[scheduled_hrs > 0].index.max()
                hr = (
                    0 if pd.isnull(hr)
                    else hr + 9 if job.specLineNo == 1  # +1 workday for final mold check
                    else hr + 1
                )
                # next jobs within the spec starts next day
                hr = math.ceil(hr / 8) * 8
                for ii in range(hr):
                    if ii not in schedule.columns:
                        schedule[ii] = 0.
                    # else:   # custom item with more than one mold
                    #     pass

            # if more than one row for manufacturing of a custom item
            elif pd.isnull(job.spec) and job['item'].startswith("и"):
                scheduled_hrs = schedule[
                    (order.itemId == job.itemId)
                    & (order.rowNo < job.rowNo)
                ].astype(bool).sum()
                hr = scheduled_hrs[scheduled_hrs > 0].index.max() + 1
            return hr

        hr = get_start_hr(order, custom_specs, schedule, job)

        # duplicated
        if hr not in schedule.columns:
            schedule[hr] = 0.

        job_allocated = 0
        pwr_rub = rates['Протяжка'] if job.shop == 'Протяжка' else job.pwr_rub
        shop = job.shop
        while job_allocated < job.pay:
            if hr not in schedule.columns:
                schedule[hr] = 0.

            shop_hrly_capa_allocated = schedule.loc[(
                order.shop == shop).values, hr].sum()
            # total_hr_capa_allocated = schedule.loc[:, hr].sum()
            available_capacity = capacity[shop] - shop_hrly_capa_allocated

            if available_capacity > 0:
                to_allocate = min(
                    available_capacity, pwr_rub, job.pay - job_allocated)
                schedule.at[i, hr] = to_allocate
                unit_prod = to_allocate / job.rate
                log.loc[len(log)] = [job.orderNo, job.rowNo,
                                     hr, to_allocate, unit_prod]
                job_allocated += to_allocate
                # shop_hrly_capa_allocated += to_allocate

            hr += 1
            if hr > 3200:
                err_msg = (
                    f'Превышен горизонт планирования: строка {job.rowNo}, '
                    + (f'{job.spec}/' if job.spec else '') + f'{job["item"]}'
                )
                print(err_msg)
                err_log.loc[len(err_log), 'err_msg'] = err_msg
                break

        print(
            f'{i=}, {job.rowNo=}, {job.pay=}, {job_allocated=}, diff={job.pay - job_allocated}')
        order.at[i, 'scheduled'] = True

        if job.pay > job_allocated:
            err_msg = (
                f'Запланирован неполный объем: строка {job.rowNo}, '
                + (f'{job.spec}/' if job.spec else '') + f'{job["item"]}, '
                f'{job_allocated}/{job.pay} руб.'
            )
            print(err_msg)
            err_log.loc[len(err_log), 'err_msg'] = err_msg

    log['timestamp'] = timestamp

    return schedule, log


def log2days(log, start):
    log['day'] = (log.hr / 8).astype(int)
    log_days = log.groupby(['order_no', 'timestamp', 'rowNo', 'day']).agg(sum)[
        ['rub_allocated', 'unit_production']]
    log_days.reset_index(drop=False, inplace=True)

    calendar = get_calendar(start)

    if start:
        dates_dict = dict(zip(range(log_days.day.max() + 1), calendar.Дата))
        log_days['date'] = log_days.day.map(dates_dict)
    else:
        log_days['date'] = None

    return log_days


def enumerate_specs(order):
    order['specId2'] = order.specId.shift()
    spec_idx = 0
    spec_idx_list = []
    for _, row in order.iterrows():
        if pd.isnull(row.specId):
            spec_idx_list += [None]
            continue

        elif row.specId != row.specId2:
            spec_idx += 1

        spec_idx_list += [spec_idx]

    order.drop(columns='specId2', inplace=True)
    order['spec_idx'] = spec_idx_list
    order.spec_idx = order.spec_idx.apply(
        lambda x: int(x) if pd.notnull(x) else None)

    return order


def schedule(
    order_no: str,
    start: str,
    timestamp: str,
    night_shift: bool,
    modelers: int,
    molders: int,
    casters: int,
    pullers: int
):

    if start:
        start = dt.strptime(start, '%d.%m.%Y')
        start = start.strftime('%Y%m%d')

    if timestamp:
        timestamp = dt.strptime(timestamp, '%d.%m.%Y %H:%M:%S')

    print('***read_order***')
    order = read_order(order_no)
    order = enumerate_specs(order)
    order = fill_power(order, night_shift)

    print('***get_schedule***')
    schedule, log = get_schedule(
        order,
        timestamp,
        night_shift,
        modelers,
        molders,
        casters,
        pullers
    )

    log_days = log2days(log, start)
    log_days.to_sql(
        name='order_daily_log',
        con=engine_analytics,
        if_exists='replace',
        index=False,
        dtype={
            'rowNo': sqlalchemy.Integer,
            'timestamp': sqlalchemy.DateTime,
            'orderNo': sqlalchemy.Text,
            'day': sqlalchemy.Integer
        }
    )

    print('***outputting***')

    log.to_sql(
        name='order_prod_sched',
        con=engine_analytics,
        if_exists='replace',
        index=False,
        dtype={
            'timestamp': sqlalchemy.DateTime,
            'orderNo': sqlalchemy.Text,
            'rowNo': sqlalchemy.Integer,
            'hr': sqlalchemy.Integer,
            'pay': sqlalchemy.Float
        }
    )

    print(f'{order.shape=}')
    print(order.iloc[:5, :6])
    print(f'{schedule.shape=}')
    print(log.head())

    err_log['timestamp'] = timestamp
    print('-'*20)
    print(f"ERR LOG, {len(err_log)} records.")
    if len(err_log) > 0:
        print(err_log)
        print('-'*20)
    err_log.to_sql(
        name='scheduling_err_log',
        con=engine_analytics,
        if_exists='replace',
        index=False,
        dtype={
            'timestamp': sqlalchemy.DateTime,
            'err_msg': sqlalchemy.Text
        }
    )

    print(f'{engine_unf=}')
