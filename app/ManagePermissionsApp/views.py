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
lakeformation_client = Session(region_name=region).client("lakeformation")

def grant_lake_formation_table_permissions(database_name, table_name, principal_arn, permissions):

    # Grant table permissions
    response = lakeformation_client.grant_permissions(
        Principal={
            'DataLakePrincipalIdentifier': principal_arn
        },
        Resource={
            'Table': {
                'DatabaseName': database_name,
                'Name': table_name
            }
        },
        Permissions=permissions
    )
    
    return response


def grant_permission(request):
    try:
        db_name = request.GET.get('db_name')
        table_name = request.GET.get('table_name')
        principal_arn = request.GET.get('principal_arn') 
        permissions = request.GET.get('permissions')
        permissions = permissions.split(',')
        response = grant_lake_formation_table_permissions(db_name, table_name, principal_arn, permissions)
        return JsonResponse({'tables': response})
    except Exception as e:
        return JsonResponse({'message': e}, status=400)


def revoke_lake_formation_table_permissions(database_name, table_name, principal_arn, permissions):
    # Create a Lake Formation client
    

    # Revoke table permissions
    response = lakeformation_client.revoke_permissions(
        Principal={
            'DataLakePrincipalIdentifier': principal_arn
        },
        Resource={
            'Table': {
                'DatabaseName': database_name,
                'Name': table_name
            }
        },
        Permissions=permissions
    )
    
    return response

def revoke_permission(request):
    try:
        db_name = request.GET.get('db_name')
        table_name = request.GET.get('table_name')
        principal_arn = request.GET.get('principal_arn') 
        permissions = request.GET.get('permissions')
        permissions = permissions.split(',')
        response = revoke_lake_formation_table_permissions(db_name, table_name, principal_arn, permissions)
        return JsonResponse({'tables': response})
    except Exception as e:
        return JsonResponse({'message': e}, status=400)
