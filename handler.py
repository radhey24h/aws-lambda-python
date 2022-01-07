import json
import boto3
import csv
import pymysql


REGION='us-east-1'
rds_host='csvdatabase.c1gev0mrg5dn.us-east-1.rds.amazonaws.com'
user_name='admin'
password='Mysql123'
db_name='csvdatabase'

s3_client=boto3.client('s3')

def lambda_handler(event, context):
    bucket=event['Records'][0]['s3']['bucket']['name']
    csv_file=event['Records'][0]['s3']['object']['key']
    csv_file_object=s3_client.get_object(Bucket=bucket, Key=csv_file)
    lines=csv_file_object['Body'].read().decode('utf-8').split()
    results=[]
    for item in csv.DictReader(lines):
        results.append(item.values())
    print(results)
    
    connection = pymysql.connect(host=rds_host, user=user_name, password=password, db='mysql')
    
    values_to_insert = [(1, 2, 'a'), (3, 4, 'b'), (5, 6, 'c')]
    query = "INSERT INTO csvdatabase.Employee (name, email, Designation) VALUES " + ",".join("(%s, %s, %s)" for _ in values_to_insert)
    flattened_values = [item for sublist in values_to_insert for item in sublist]

    
    cursor=connection.cursor()
    cursor.execute(query, flattened_values);
    connection.commit()

    print(cursor.rowcount, 'Record inserted successfully into employee table.!')

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
