import configparser
import boto3
import json
import time

import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def configuration():
    """
        This function will configure all the variables needed for later cluster creation, and all variables configured
        will be global, so they could be used in any function inside this file.
        
        Parameters: None
        Return: None
    """
    
    global KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, \
           DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, \
           DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME, \
           DWH_HOST, ARN
    print('--------------- Configuration --------------\n')
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    #Configuration for key id and access key
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    
    #Configure DHW
    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    
    #Configure CLUSTER
    DWH_DB                 = config.get("CLUSTER","DWH_DB")
    DWH_DB_USER            = config.get("CLUSTER","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("CLUSTER","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("CLUSTER","DWH_PORT")
    DWH_HOST               = config.get("CLUSTER", "HOST")
    
    #Configure IAM_ROLE
    ARN                    = config.get("IAM_ROLE", "ARN")
    
    
def create_client():
    """
        This function will create clients for ec2, iam, and redshift, and return the clients.
        
        Parameters: None
        Return: ec2 object, iam object, redshift object
    """
    print('--------------- Creating Resource and Client --------------\n')
    ec2 = boto3.resource('ec2',
                        region_name = 'us-west-2',
                        aws_access_key_id = KEY,
                        aws_secret_access_key = SECRET)

    iam = boto3.client('iam',
                        region_name = 'us-west-2',
                        aws_access_key_id = KEY,
                        aws_secret_access_key = SECRET)
    
    redshift = boto3.client('redshift',
                             region_name = 'us-west-2',
                             aws_access_key_id = KEY,
                             aws_secret_access_key = SECRET)
    
    return ec2, iam, redshift

def create_iam_role(iam, iam_role_name):
    """
        Create an IAM role that makes Redshift able to access bucket (ReadOnly)
        
        Parameters: iam Object, 
                    iam role name Object
        Return: roleArn
    """
    # STEP 1
    try:
        print("--------------- Creating a new IAM Role --------------\n")
        dwhRole = iam.create_role(
            Path='/',
            RoleName = iam_role_name,
            Description = 'Allows Redshift clusters to call AWS services on my behalf',
            AssumeRolePolicyDocument=json.dumps(
             {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'}
            )
        )

    except Exception as e:
        print('Create IAM Role Error: \n',e)
        
    
    # STEP 2
    print('---------------- Attach Policy --------------\n')
    status_code = iam.attach_role_policy(
        RoleName = iam_role_name,
        PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']
    
    print('Status Code: {}'.format(status_code))
    
    
    # STEP 3
    print('---------------- Get the IAM Role Arn ----------------\n')
    roleArn = iam.get_role(RoleName=iam_role_name)['Role']['Arn']
    
    return roleArn
    
    
def create_redshift_cluster(redshift, roleArn):
    """
        Create redshift cluster.
        Parameters: redshift Object, 
                    roleArn Cluster name
        Return: None
    """
    print('---------------- Creating Cluster ----------------\n')
    try:
        redshift.create_cluster(
            # Add parameter for configuration
            NodeType = DWH_NODE_TYPE,
            NumberOfNodes = int(DWH_NUM_NODES),
            ClusterType = DWH_CLUSTER_TYPE,
            
            # Add parameters for identifieres & credentials
            DBName = DWH_DB,
            MasterUsername = DWH_DB_USER,
            MasterUserPassword = DWH_DB_PASSWORD,
            ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
            
            # Add parameters for role to allow s3 access
            IamRoles = [roleArn]
        )
    except Exception as e:
        print('Create Redshift Cluster Error: \n',e)
    
    
def check_cluster_availability(redshift, cluster_identifier):
    """
        This function checks the current cluster status
    """
    clusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    return clusterProps['ClusterStatus'].lower()
    
def open_tcp_port(ec2, redshift):
    
    """
        This function opens an incoming TCP port to access the cluster endpoint
    """
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
    
        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name, 
            CidrIp='0.0.0.0/0', 
            IpProtocol='TCP',  
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
    
    
def main():
    
    configuration()
    ec2, iam, redshift = create_client()
    roleArn = create_iam_role(iam=iam, iam_role_name=DWH_IAM_ROLE_NAME)
    create_redshift_cluster(redshift, roleArn)

    
    while check_cluster_availability(redshift, DWH_CLUSTER_IDENTIFIER) != 'available':
        print('Cluster not ready yet, wait for 60 secondes \n')
        time.sleep(60)

    print('Current Cluster Status: ', check_cluster_availability(redshift, DWH_CLUSTER_IDENTIFIER))
    print('Cluster Create Successfully \n')
    
    clusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    #write Endpoint and roleARN in the configration file
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    config.set("CLUSTER", "HOST", clusterProps['Endpoint']['Address'])
    config.set("IAM_ROLE", "ARN", clusterProps['IamRoles'][0]['IamRoleArn'])

    with open('dwh.cfg', 'w+') as configfile:
        config.write(configfile)
    
    # Re-run configuration to set parameters
    configuration()
    
    # Open an TCP port to access the cluster endpoint
    open_tcp_port(ec2, redshift)

    # Connect to the database    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Connected')

    conn.close()
    print('Disconnected')

if __name__ == "__main__":
    main()