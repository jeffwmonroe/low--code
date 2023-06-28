import pandas as pd
import sqlalchemy as sqla
import os
import pickle


# ToDo build these into environment variables or pass them as parameters
def redshift():
    dialect = "redshift"
    # driver = "psycopg2"
    driver = "redshift_connector"

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


# metadata_pickle_filename = "mydb_metadata"
# cache_path = ".sqlalchemy_cache"
# print(f'cache_path = {cache_path}')
#
# metadata_obj = None
# if os.path.exists(cache_path):
#     try:
#         with open(os.path.join(cache_path, metadata_pickle_filename), 'rb') as cache_file:
#             print(f'Loading metadata from cache')
#             metadata_obj = pickle.load(file=cache_file)
#             metadata_obj.bind(engine)
#             print('cache loaded')
#     except:
#         # cache file not found - no problem, reflect as usual
#         print('Cache file not found')
#         pass
#
# hack = 0


def table_loader(table_name, metadata_obj) -> bool:
    global hack
    print(f'loading: {table_name}')
    hack = hack + 1
    return hack <= 10


#
# if metadata_obj is None:
#     print('loading metadata')
#     metadata_obj = sqla.MetaData(schema='carc_data')
#
#     metadata_obj.reflect(bind=engine,
#                          views=True,
#                          resolve_fks=False,
#                          )
#
#     # save the metadata for future runs
#     try:
#         if not os.path.exists(cache_path):
#             os.makedirs(cache_path)
#         # make sure to open in binary mode - we're writing bytes, not str
#         with open(os.path.join(cache_path, metadata_pickle_filename), 'wb') as cache_file:
#             pickle.dump(metadata_obj, cache_file)
#     except:
#         # couldn't write the file for some reason
#         print('could not write meta data')
#         pass

# metadata_obj.reflect(bind=engine)
print('-' * 50)
# print(metadata_obj.tables.items())
# for tab in metadata_obj.tables.values():
#     print(f'table = {tab.name}')
#     table = metadata_obj.tables['carc_data.' + tab.name]
#     print(f'   num cols = {len(table.columns)}')
#     for col in table.columns:
#         print(f'   col = {col.name}')
#         print(f'      col type= {col.type}')

#     # self.metadata_obj = sqla.MetaData(schema=self.schema)

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
    print(f'get_data: {data}')
    table = sqla.Table(data['table_name'], metadata, autoload_with=engine)
    # table = metadata.tables[data['table_name']]
    stmt = (sqla.select(table.c[data['column_name']])
            .group_by(table.c[data['column_name']])
            .filter(table.c[data['column_name']] != None))
    with engine.connect() as conn:
        result = conn.execute(stmt)
        row_list: list[dict[str, str | int]] = [row._asdict() for row in result]

    print('-' * 50)
    print('row_list')
    print(f'len = {len(row_list)}')
    first = row_list[0]
    count = 0
    for row in row_list:
        print(f'row = {row}')
        count = count + 1
        if count > 20:
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
    row_list = get_data(engine, metadata, data)
    values = [{'external_name': row[data['column_name']], 'external_id': row[data['column_name']]} for row in row_list]
    put_data(ontology_engine, ontology_metadata, data, values)
    # put_data_excel(data, values)
