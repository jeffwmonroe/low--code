import pandas as pd
import sqlalchemy as sqla
import time
from low_code_pipeline.data_source import Redshift, LocalOntology, MScienceOntology

redshift = Redshift()
url = redshift.url()
print(f'url = {url}')
engine = sqla.create_engine(url)
# engine = sqla.create_engine(url)

with engine.connect() as conn:
    conn.execute(sqla.text("select 'hello world'"))
    print("Successful login. Database exists. Connection good...")

print('-' * 50)

data_list = [
    {'table_name': 'connected_tv_dma_actor_title_agg',
     'column_name': 'actor',
     'dataset_name': "connected_tv_dma",
     'entity_type': 'actor'
     },
    {'table_name': 'ec_brand_consumer_spend_week',
     'column_name': 'brand_name',
     'dataset_name': "ec_brand_consumer",
     'entity_type': 'brand'
     },
    {'table_name': 'concert_event_estimate_and_actuals',
     'column_name': 'artist_name',
     'dataset_name': "concert_event",
     'entity_type': 'artist'
     },
]


def get_data(engine, metadata, data):
    print('-' * 50)
    print(f'get_data: {data}')
    table = sqla.Table(data['table_name'], metadata, autoload_with=engine)
    print(f"   number of columns: {len(table.c.keys())}")
    for col in table.c.keys():
        print(f'   col = {col}')
    # table = metadata.tables[data['table_name']]
    stmt = (sqla.select(table.c[data['column_name']])
            .group_by(table.c[data['column_name']])
            .filter(table.c[data['column_name']] != None))
    print(f'stmt:   {stmt}')
    start = time.time()
    with engine.connect() as conn:
        result = conn.execute(stmt)
        row_list: list[dict[str, str | int]] = [row._asdict() for row in result]
    end = time.time()
    print(f'query time: {end - start}')
    print('-' * 50)
    print('row_list')
    print(f'   len = {len(row_list)}')
    first = row_list[0]
    count = 0
    for row in row_list:
        print(f'row = {row}')
        count = count + 1
        if count > 5:
            break
    return row_list


def get_data2(data):
    print('-' * 50)
    print(f'get_data: {data}')
    table = sqla.Table(data['table_name'], metadata, autoload_with=engine)
    print(f"   number of columns: {len(table.c.keys())}")
    for col in table.c.keys():
        print(f'   col = {col}')
    # table = metadata.tables[data['table_name']]
    stmt = (sqla.select(table.c[data['column_name']])
            .group_by(table.c[data['column_name']])
            .filter(table.c[data['column_name']] != None))
    print(f'stmt:   {stmt}')
    start = time.time()
    with engine.connect() as conn:
        result = conn.execute(stmt)
        row_list: list[dict[str, str | int]] = [row._asdict() for row in result]
    end = time.time()
    print(f'query time: {end - start}')
    print('-' * 50)
    print('row_list')
    print(f'   len = {len(row_list)}')
    first = row_list[0]
    count = 0
    for row in row_list:
        print(f'row = {row}')
        count = count + 1
        if count > 5:
            break
    return row_list


def put_data_excel(data, row_list):
    print(f'data={data}')
    df = pd.DataFrame(row_list)
    df.columns = ['external_name', 'external_id']
    print(df)
    df.to_csv(f'external_{data["entity_type"]}.csv')


def put_data(engine, metadata, data, values):
    print(f'put_data: {data}')

    table_name = f'external_{data["dataset_name"]}_{data["entity_type"]}'
    table = sqla.Table(table_name, metadata, autoload_with=engine)

    delete_stmt = sqla.delete(table)

    with engine.connect() as conn:
        conn.execute(delete_stmt)
        stmt = table.insert()
        conn.execute(stmt, values)


# url_ontology = mscience_ontology()
ontology = LocalOntology()
url_ontology = ontology.url()
print(f'url = {url_ontology}')
ontology_engine = sqla.create_engine(url_ontology)

with ontology_engine.connect() as conn:
    conn.execute(sqla.text("select 'hello world'"))
    print("Successful ontology login. Database exists. Connection good...")
ontology_metadata = sqla.MetaData(schema='ontology')


mscience = MScienceOntology()
url_mscience = mscience.url()
print(f'url = {url_mscience}')
mscience_engine = sqla.create_engine(url_mscience)

with mscience_engine.connect() as conn:
    conn.execute(sqla.text("select 'hello world'"))
    print("Successful ontology login. Database exists. Connection good...")
mscience_metadata = sqla.MetaData(schema='ontology')

def read_all():
    metadata = sqla.MetaData(schema='carc_data')
    for data in data_list:
        print(f'data = {data}')
        row_list = get_data(engine, metadata, data)
        values = [{'external_name': row[data['column_name']],
                   'external_id': row[data['column_name']]} for row in row_list]
        count = 0
        for row in values:
            print(f'row = {row}')
            count = count + 1
            if count > 5:
                break
        # put_data(ontology_engine, ontology_metadata, data, values)
        put_data_excel(data, values)


def main():
    for data in data_list:
        print(data['entity_type'])
        csv_name = f'mapped_{data["entity_type"]}.csv'
        df = pd.read_csv(csv_name)
        df = df[['external_name', 'external_id']]
        print(df)
        values = df.to_dict(orient="records")
        # print(values)
        print(f'value[0] ={values[0]}')
        # for row in values:
        #     print(f'row={row}')
        put_data(mscience_engine, mscience_metadata, data, values)


if __name__ == "__main__":
    main()
