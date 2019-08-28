from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_conn_id=None,
                 redshift_conn_id="redshift",
                 create_table_name=None,
                 create_table_query=None,
                 copy_from_s3_query=None,
                 s3_path=None,
                 json=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.create_table_query = create_table_query
        self.create_table_name = create_table_name
        self.copy_from_s3_query = copy_from_s3_query
        self.s3_path = s3_path
        self.json=json

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.hook.run(self.create_table_query)
        self.log.info("CREATE {} TABLE SUCCESS".format(self.create_table_name))
        
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        credentials = self.s3.get_credentials()
        self.hook.run(self.copy_from_s3_query.format(self.create_table_name,self.s3_path,credentials.access_key,credentials.secret_key, self.json))
        self.log.info("COPY {} TABLE SUCCESS".format(self.create_table_name))
    




