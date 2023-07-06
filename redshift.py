import pandas as pd
import sqlalchemy as sqla
import time
import os
import pickle


# ToDo build these into environment variables or pass them as parameters
def redshift():
    # dialect = "redshift"
    dialect = "postgresql"
    driver = "psycopg2"
    # driver = "redshift_connector"

    user = "mscience"
    password = "Xo77BkAr2t9zxzsf38G9QEWF"
    host = "carc-mscience-dev-rs-wg.573033409598.us-east-1.redshift-serverless.amazonaws.com"
    port = "5439"
    name = "carc_mscience_db"
    # url = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{name}"
    url = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{name}"
    return url


def local_ontology():
    dialect = "postgresql"
    driver = "psycopg2"

    user = "engineering"
    password = "none"
    host = "localhost"
    port = "5432"
    name = "ontology"
    # url = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{name}"
    url = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{name}"
    return url


def mscience_ontology():
    dialect = "postgresql"
    driver = "psycopg2"

    user = "ontology"
    password = "QxNpRxUGyzbnq4LvFrXXNsZY"
    host = "engineering-dev-db-public-65ed91db46048f85.elb.us-east-1.amazonaws.com"
    port = "5432"
    name = "ontology"
    # url = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{name}"
    url = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{name}"
    return url


# url = db_url()

# try:
#     conn = psycopg2.connect(dbname=name,
#                             host=host,
#                             port=port,
#                             user=user,
#                             password=password)
# except Excep
#     print('error connection')

url = redshift()
print(f'url = {url}')
engine = sqla.create_engine(url)
# engine = sqla.create_engine(url)

with engine.connect() as conn:
    conn.execute(sqla.text("select 'hello world'"))
    print("Successful login. Database exists. Connection good...")


print('-' * 50)

data_list = [{'table_name': 'concert_event_estimate_and_actuals',
              'column_name': 'artist_name',
              'dataset_name': "rose",
              'entity_type': 'artist'
              },
             {'table_name': 'connected_tv_dma_actor_title_agg',
              'column_name': 'actor',
              'dataset_name': "daisy",
              'entity_type': 'actor'
              },
             {'table_name': 'ec_brand_consumer_spend_week',
              'column_name': 'brand_name',
              'dataset_name': "tulip",
              'entity_type': 'brand'
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
            # .group_by(table.c[data['column_name']])
            .filter(table.c[data['column_name']] != None))
    print(f'stmt:   {stmt}')
    start = time.time()
    with engine.connect() as conn:
        result = conn.execute(stmt)
        row_list: list[dict[str, str | int]] = [row._asdict() for row in result]
    end = time.time()
    print(f'query time: {end-start}')
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
url_ontology = local_ontology()
print(f'url = {url_ontology}')
ontology_engine = sqla.create_engine(url_ontology)

with ontology_engine.connect() as conn:
    conn.execute(sqla.text("select 'hello world'"))
    print("Successful ongology login. Database exists. Connection good...")
ontology_metadata = sqla.MetaData(schema='ontology')

metadata = sqla.MetaData(schema='carc_data')
for data in data_list:
    print(f'data = {data}')
    row_list = get_data(engine, metadata, data)
    values = [{'external_name': row[data['column_name']], 'external_id': row[data['column_name']]} for row in row_list]
    # put_data(ontology_engine, ontology_metadata, data, values)
    # put_data_excel(data, values)
