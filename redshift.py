import sqlalchemy as sqla

# ToDo build these into environment variables or pass them as parameters
dialect = "postgresql"
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

# url = db_url()

# try:
#     conn = psycopg2.connect(dbname=name,
#                             host=host,
#                             port=port,
#                             user=user,
#                             password=password)
# except Excep
#     print('error connection')

print(f'url = {url}')
engine = sqla.create_engine(url,
                            # echo=True,
                            )
# engine = sqla.create_engine(url)

with engine.connect() as conn:
    conn.execute(sqla.text("select 'hello world'"))
    print("Successful login. Database exists. Connection good...")

metadata_obj = sqla.MetaData(schema='carc_data')

hack = 0


def table_loader(table_name, metadata_obj) -> bool:
    global hack
    print(f'loading: {table_name}')
    # hack = hack + 1
    return hack <= 1


metadata_obj.reflect(bind=engine,
                     views=True,
                     resolve_fks=False,
                     only=table_loader)

# metadata_obj.reflect(bind=engine)
print('-' * 50)
# print(metadata_obj.tables.items())
for tab in metadata_obj.tables.values():
    print(f'table = {tab.name}')
    table = metadata_obj.tables['carc_data.' + tab.name]
    print(f'   num cols = {len(table.columns)}')
    for col in table.columns:
        print(f'   col = {col.name}')
        print(f'      col type= {col.type}')

#     # self.metadata_obj = sqla.MetaData(schema=self.schema)
