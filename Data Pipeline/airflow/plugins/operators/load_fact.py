from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 truncate_table=False,
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.table=table
        self.truncate_table=truncate_table

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f'Truncate content from table {self.table}')
            redshift.run(f'TRUNCATE {self.table}')
            
        self.log.info('Loading Fact Table')
        redshift.run(self.sql_query)
        
        self.log.info('Fact Table Has Been Loaded')