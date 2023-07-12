class DataSource:
    def __init__(self,
                 dialect=None,
                 driver=None,
                 user=None,
                 password=None,
                 host=None,
                 port=None,
                 database=None
                 ):
        self.dialect = dialect
        self.driver = driver
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def url(self):
        url = f"{self.dialect}+{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return url


# ToDo build these into environment variables or pass them as parameters
class Redshift(DataSource):
    def __init__(self):
        super().__init__(
            dialect="redshift",
            driver="psycopg2",
            user="mscience",
            password="Xo77BkAr2t9zxzsf38G9QEWF",
            host="carc-mscience-dev-rs-wg.573033409598.us-east-1.redshift-serverless.amazonaws.com",
            port="5439",
            database="carc_mscience_db"
        )
    # url = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{name}"
    # return url


class LocalOntology(DataSource):
    def __init__(self):
        super().__init__(
            dialect="postgresql",
            driver="psycopg2",
            user="engineering",
            password="none",
            host="localhost",
            port="5432",
            database="ontology"
        )
    # url = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{name}"
    # return url


class MScienceOntology(DataSource):
    def __init__(self):
        super().__init__(
            dialect="postgresql",
            driver="psycopg2",
            user="ontology",
            password="QxNpRxUGyzbnq4LvFrXXNsZY",
            host="engineering-dev-db-public-65ed91db46048f85.elb.us-east-1.amazonaws.com",
            port="5432",
            database="ontology"
        )
    # url = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{name}"
    # return url
