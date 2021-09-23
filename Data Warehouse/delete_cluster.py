import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from create_cluster import check_cluster_availability
import boto3
import time

def delete_cluster(redshift, cluster_identifier):
    if check_cluster_availability(redshift, cluster_identifier) == 'available':
        try:
            print('Start Deleting the cluster!')
            response = redshift.delete_cluster(ClusterIdentifier=cluster_identifier,  SkipFinalClusterSnapshot=True)
            time.sleep(60)
            print('Current Status: ', check_cluster_availability(redshift, cluster_identifier))
        except Exception as e:
            print(e)
        
    else:
        try:
            print('Current Status: ', check_cluster_availability(redshift, cluster_identifier))
        except Exception as e:
            print(e)


def delete_iam(iam, iam_role_name):
    print('Start Deleting IAM Role')
    iam.detach_role_policy(RoleName=iam_role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

    iam.delete_role(RoleName = iam_role_name)
def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    #Configuration for key id and access key
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    DWH_IAM_ROLE_NAME = config.get('DWH', 'DWH_IAM_ROLE_NAME')
    
    redshift = boto3.client('redshift',
                             region_name = 'us-west-2',
                             aws_access_key_id = KEY,
                             aws_secret_access_key = SECRET)
    iam = boto3.client('iam',
                             region_name = 'us-west-2',
                             aws_access_key_id = KEY,
                             aws_secret_access_key = SECRET)
    
    delete_cluster(redshift, DWH_CLUSTER_IDENTIFIER)
    delete_iam(iam, DWH_IAM_ROLE_NAME)
    
if __name__ == "__main__":
    main()