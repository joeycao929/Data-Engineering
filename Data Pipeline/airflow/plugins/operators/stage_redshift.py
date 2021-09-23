from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS json '{}'
        TIMEFORMAT as 'epochmillisecs'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 log_json_file="",
                 *args, 
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.log_json_file = log_json_file

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        
        self.log.info('Clearing data from destination Redshift table')
        redshift.run("TRUNCATE {}".format(self.table))
        
#         rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f'Staging Table {self.table} from {s3_path}')
        
        if self.log_json_file != "":
            json_file = "s3://{}/{}".format(self.s3_bucket, self.log_json_file)
            copy_query = self.copy_sql.format(self.table, 
                                              s3_path, 
                                              credentials.access_key,
                                              credentials.secret_key,
                                              self.region,
                                              json_file)
        else:
            copy_query = self.copy_sql.format(self.table, 
                                              s3_path, 
                                              credentials.access_key,
                                              credentials.secret_key,
                                              self.region,
                                              'auto')
        
        self.log.info(f'Runing copy query: {copy_query}')
        redshift.run(copy_query)
        
        self.log.info(f'Complete staging table {self.table}')
