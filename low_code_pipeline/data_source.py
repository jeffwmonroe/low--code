from low_code_pipeline.node import Node


class DataSource(None):
    def __init__(self):
        req = ['dialect',
               'driver',
               'user',
               'password',
               'host',
               'port',
               'database',
               ]
        super().__init__(req)
        self.data = {
            'dialect': "redshift",
            'driver': "redshift_connector",
            'user': "mscience",
            'password': "Xo77BkAr2t9zxzsf38G9QEWF",
            'host': "carc-mscience-dev-rs-wg.573033409598.us-east-1.redshift-serverless.amazonaws.com",
            'port': "5439",
            'database': "carc_mscience_db",
        }
