from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id=None,
                 create_table_name=None,
                 create_table_query=None,
                 insert_table_query=None,
                 append_data=None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.create_table_name = create_table_name
        self.redshift_conn_id = redshift_conn_id
        self.create_table_query = create_table_query
        self.insert_table_query = insert_table_query
        self.append_data = append_data

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.hook.run(self.create_table_query)
        self.log.info("PROCESS CREATE {} TABLE SUCCESS".format(self.create_table_name))
        if self.append_data == True:        
            self.hook.run(self.insert_table_query)
            self.log.info("PROCESS INSERT IN {} TABLE SUCCESS".format(self.create_table_name))
        else:
            self.hook.run("DELETE FROM {}".format(self.create_table_name))
            self.hook.run(self.insert_table_query)
            self.log.info("PROCESS INSERT IN {} TABLE SUCCESS".format(self.create_table_name))            
        