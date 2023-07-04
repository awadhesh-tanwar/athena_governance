import json

import boto3
from django.http import JsonResponse, HttpResponse
from django.shortcuts import render
from app.settings import AWS_REGION_NAME
from boto3.session import *
import logging

region = AWS_REGION_NAME
iam_client = Session(region_name=region).client("iam")
glue_client = Session(region_name=region).client("glue")

def list_principals():
    # iam_client = boto3.client('iam')
    response = iam_client.list_users()
    users = response['Users']
    response = iam_client.list_roles()
    roles = response['Roles']
    user_names = [{"user_name":user['UserName'], "arn":user['Arn']} for user in users]
    role_names = [{"role_name":role['RoleName'], "arn":role['Arn']} for role in roles]
    return (user_names, role_names)


def get_users(request):
    try:
        user_names, role_names = list_principals()
        return JsonResponse({'users': user_names, 'roles': role_names})
    except Exception as e:
        return JsonResponse({'message': e}, status=400)


def get_dbs(request):
    try:
        response = glue_client.get_databases()
        databases = response['DatabaseList']
        return JsonResponse({'dbs': databases})
    except Exception as e:
        return JsonResponse({'message': e}, status=400)


def get_tables(request):
    try:
        db_name = request.GET.get('db_name')
        json_data = glue_client.get_tables(DatabaseName=db_name)
        tables = [table['Name'] for table in json_data['TableList']]
        return JsonResponse({'tables': tables})
    except Exception as e:
        return JsonResponse({'message': e}, status=400)
