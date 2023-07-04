from django.shortcuts import render

# Create your views here.
from django.http import JsonResponse
from datetime import datetime
import time
import awswrangler
from pandas import json_normalize, DataFrame
from boto3.session import *
from pandas import json_normalize
from typing import Tuple, List, Dict
import pandas as pd
import requests

from app.settings import AWS_REGION_NAME
from .utils.athena_utils import dump_table_permissions_table_into_athena, dump_db_permissions_table_into_athena
# Retrieves the list of permissions for given resource type
region = AWS_REGION_NAME
lf_client = Session(region_name=region).client("lakeformation")


def _get_permissions(catalog_id: str, resource_type: str = None, next_token: str = "") -> Tuple[str, DataFrame]:
    # print("_get_permissions :: start")
    # print(f"Next token before = {next_token}")
    request = {"CatalogId": catalog_id, "NextToken": next_token, "MaxResults": 500}
    if resource_type:
        request["ResourceType"] = resource_type
    list_permission = lf_client.list_permissions(**request)
    next_token = list_permission.get("NextToken")
    df = json_normalize(list_permission, "PrincipalResourcePermissions")
    # print(f"Next token after = {next_token}")
    # print("_get_permissions :: end")
    return next_token, df


def download_db_permissions(catalog_id: str, resource_type: str):
    next_token = ""
    all_columns = ['db_name', 'db_arn', 'principal', 'user_name', 'p_ALL', 'p_ALTER', 'p_CREATE_TABLE', 'p_DESCRIBE',
                   'p_DROP']
    final_df = pd.DataFrame(columns=all_columns)

    while True:
        next_token, df = _get_permissions(catalog_id=catalog_id, resource_type=resource_type, next_token=next_token)
        if df.size == 0:
            print("download_resource_named_resourcetype for {} :: Empty Result".format(resource_type))
        else:
            new_df = pd.DataFrame(columns=all_columns)
            new_df['db_name'] = df['Resource.Database.Name']
            new_df['principal'] = df['Principal.DataLakePrincipalIdentifier']
            new_df['user_name'] = new_df['principal'].apply(lambda x: x.split('/')[-1])
            for i, row in df.iterrows():
                permissions = row['Permissions']
                for permission in permissions:
                    new_df.loc[i, 'p_' + permission] = True
            new_df.fillna(False, inplace=True)
            final_df = pd.concat([final_df, new_df])
        if next_token is None:
            break
    return final_df


region = 'ap-south-1'
catalog_id = '712268157070'
s3bucket = 'ds-lake-sbox-test-s3'
databasename = 'default'
createtable = 'yes'


def db_view(request):
    df = download_db_permissions(catalog_id, "DATABASE")
    print("hello")
    # DBPermissions.objects.all().delete()
    # data = df.to_dict('records')
    # instances = [DBPermissions(**row) for row in data]
    # DBPermissions.objects.bulk_create(instances)
    res = dump_db_permissions_table_into_athena(df)
    if res:
        return JsonResponse({'message': 'DBs updated successfully'})
    else:
        return JsonResponse({'message': 'Failed to update DBs'}, status=400)


def download_table_permissions(catalog_id: str, resource_type: str = "TABLE"):
    next_token = ""
    all_columns = ['db_name', 'table_name', 'table_arn', 'principal', 'user_name', 'p_ALL', 'p_ALTER', 'p_DELETE',
                   'p_DESCRIBE', 'p_DROP', 'p_INSERT', 'p_SELECT']
    final_df = pd.DataFrame(columns=all_columns)

    while True:
        next_token, df = _get_permissions(catalog_id=catalog_id, resource_type="TABLE", next_token=next_token)
        if df.size == 0:
            print("download_resource_named_resourcetype for {} :: Empty Result".format(resource_type))
        else:
            new_df = pd.DataFrame(columns=all_columns)
            new_df['db_name'] = df['Resource.Table.DatabaseName'].fillna(
                df['Resource.TableWithColumns.DatabaseName'])  # df['Resource.Table.DatabaseName']
            new_df['table_name'] = df['Resource.Table.Name'].fillna(df['Resource.TableWithColumns.Name'])
            new_df['principal'] = df['Principal.DataLakePrincipalIdentifier']
            new_df['user_name'] = new_df['principal'].apply(lambda x: x.split('/')[-1])
            for i, row in df.iterrows():
                permissions = row['Permissions']
                for permission in permissions:
                    new_df.loc[i, 'p_' + permission] = True
            new_df.fillna(False, inplace=True)
            final_df = pd.concat([final_df, new_df])
        if next_token is None:
            break
    final_df = final_df.groupby(['db_name', 'table_name', 'table_arn', 'principal', 'user_name']).agg(
        p_all=('p_ALL', 'max'),
        p_alter=('p_ALTER', 'max'),
        p_DELETE=('p_DELETE', 'max'),
        p_DESCRIBE=('p_DESCRIBE', 'max'),
        p_DROP=('p_DROP', 'max'),
        p_INSERT=('p_INSERT', 'max'),
        p_SELECT=('p_SELECT', 'max')
    ).reset_index().sort_values('db_name')
    return final_df


def table_view(request):
    df = download_table_permissions(catalog_id, "TABLE")
    # TablePermissions.objects.all().delete()
    # data = df.to_dict('records')
    # instances = [TablePermissions(**row) for row in data]
    # TablePermissions.objects.bulk_create(instances)
    res = dump_table_permissions_table_into_athena(df)
    if res:
        return JsonResponse({'message': 'Tables updated successfully'})
    else:
        return JsonResponse({'message': 'Failed to update Tables'}, status=400)


def _get_resource_by_lf_tags(catalog_id: str, lf_tag_expression: List, resource_type: str, next_token: str = "") -> \
Tuple[str, DataFrame]:
    print("_get_resource_by_lf_tags :: start")
    print(f"Searching for expression = {lf_tag_expression}, resource_type = {resource_type}")
    if resource_type == "DATABASE":
        response = lf_client.search_databases_by_lf_tags(Expression=lf_tag_expression, CatalogId=catalog_id,
                                                         NextToken=next_token)
        next_token = response.get("NextToken")
        df = json_normalize(response, "DatabaseList")
    elif resource_type == "TABLE":
        response = lf_client.search_tables_by_lf_tags(Expression=lf_tag_expression, CatalogId=catalog_id,
                                                      NextToken=next_token)
        next_token = response.get("NextToken")
        df = json_normalize(response, "TableList")
    else:
        print("[ERROR]: _get_resource_by_lf_tags :: Invalid resource type {}:".format(resource_type))
    pd.set_option('display.max_columns', None)
    print("_get_resource_by_lf_tags :: end")
    return next_token, df


def download_lftags_db_mapping(catalog_id: str, resource_type: str):
    next_token = ""
    all_columns = ['lftag_key', 'lftag_value', 'db_name', 'db_arn', 'principal', 'user_name', 'p_ALL', 'p_ALTER',
                   'p_CREATE_TABLE', 'p_DESCRIBE', 'p_DROP']
    final_df = pd.DataFrame(columns=all_columns)
    while True:
        next_token, df = _get_permissions(catalog_id=catalog_id, resource_type=resource_type, next_token=next_token)
        print(df.columns)
        if df.size == 0:
            print("download_resource_named_resourcetype for {} :: Empty Result".format(resource_type))
        else:
            df = df[['Permissions', 'Principal.DataLakePrincipalIdentifier', 'Resource.LFTagPolicy.Expression']]
            df['Resource.LFTagPolicy.Expression'] = df['Resource.LFTagPolicy.Expression'].astype(str)
            for index, row in df.iterrows():
                expression = list(eval(row['Resource.LFTagPolicy.Expression']))
                permissions = row['Permissions']
                principal = row['Principal.DataLakePrincipalIdentifier']
                lfresourcetype = 'DATABASE'
                lf_next_token = ""
                while True:
                    lf_next_token, iter_df = _get_resource_by_lf_tags(catalog_id=catalog_id,
                                                                      lf_tag_expression=expression,
                                                                      resource_type=lfresourcetype,
                                                                      next_token=lf_next_token)
                    print(iter_df.columns)
                    if iter_df.size == 0:
                        print("_get_resource_by_lf_tags for {} :: Empty Result".format(expression))
                    else:
                        # new_df = pd.DataFrame(columns=all_columns)
                        # new_df['db_name'] = iter_df['Database.Name']
                        # new_df['principal'] = principal
                        # new_df['user_name'] = principal.split('/')[-1]
                        # for i, row2 in iter_df.iterrows():
                        #     for permission in permissions:
                        #         new_df.loc[i, 'p_' + permission] = True
                        new_df = pd.DataFrame(columns=all_columns)
                        new_df['db_name'] = iter_df['Database.Name']
                        new_df['principal'] = principal
                        new_df['user_name'] = principal.split('/')[-1]
                        # permissions = iter_df.apply(
                        #     lambda row: [permission for permission in permissions if permission], axis=1)
                        permission_columns = ['p_' + permission for permission in permissions]
                        new_df[permission_columns] = True
                        new_df.fillna(False, inplace=True)
                        final_df = pd.concat([final_df, new_df])
                    if lf_next_token is None:
                        break
        if next_token is None:
            break
    return final_df


def download_lftags_db_mapping2(catalog_id: str, resource_type: str):
    next_token = ""
    all_columns = ['lftag_key', 'lftag_value', 'db_name', 'db_arn', 'principal', 'user_name', 'p_ALL', 'p_ALTER',
                   'p_CREATE_TABLE', 'p_DESCRIBE', 'p_DROP']
    final_df = pd.DataFrame(columns=all_columns)
    while True:
        next_token, df = _get_permissions(catalog_id=catalog_id, resource_type=resource_type, next_token=next_token)
        print(df.columns)
        if df.size == 0:
            print("download_resource_named_resourcetype for {} :: Empty Result".format(resource_type))
        else:
            df = df[['Permissions', 'Principal.DataLakePrincipalIdentifier', 'Resource.LFTagPolicy.Expression']]
            df['Resource.LFTagPolicy.Expression'] = df['Resource.LFTagPolicy.Expression'].astype(str)
            for index, row in df.iterrows():
                expression = list(eval(row['Resource.LFTagPolicy.Expression']))
                permissions = row['Permissions']
                principal = row['Principal.DataLakePrincipalIdentifier']
                lfresourcetype = 'DATABASE'
                lf_next_token = ""
                while True:
                    lf_next_token, iter_df = _get_resource_by_lf_tags(catalog_id=catalog_id,
                                                                      lf_tag_expression=expression,
                                                                      resource_type=lfresourcetype,
                                                                      next_token=lf_next_token)
                    print(iter_df.columns)
                    if iter_df.size == 0:
                        print("_get_resource_by_lf_tags for {} :: Empty Result".format(expression))
                    else:
                        # new_df = pd.DataFrame(columns=all_columns)
                        # new_df['db_name'] = iter_df['Database.Name']
                        # new_df['principal'] = principal
                        # new_df['user_name'] = principal.split('/')[-1]
                        # for i, row2 in iter_df.iterrows():
                        #     for permission in permissions:
                        #         new_df.loc[i, 'p_' + permission] = True
                        new_df = pd.DataFrame(columns=all_columns)
                        new_df['db_name'] = iter_df['Database.Name']
                        new_df['principal'] = principal
                        new_df['user_name'] = principal.split('/')[-1]
                        # permissions = iter_df.apply(
                        #     lambda row: [permission for permission in permissions if permission], axis=1)
                        permission_columns = ['p_' + permission for permission in permissions]
                        new_df[permission_columns] = True
                        new_df.fillna(False, inplace=True)
                        final_df = pd.concat([final_df, new_df])
                    if lf_next_token is None:
                        break
        if next_token is None:
            break
    return final_df


def lftags_db_view(request):
    df = download_lftags_db_mapping2(catalog_id, "LF_TAG_POLICY_DATABASE")
    # TablePermissions.objects.all().delete()
    # data = df.to_dict('records')
    # instances = [TablePermissions(**row) for row in data]
    # TablePermissions.objects.bulk_create(instances)
    res = dump_lftags_db_table_into_athena(df)
    if res:
        return JsonResponse({'message': 'LFTagsMapping updated successfully'})
    else:
        return JsonResponse({'message': 'Failed to update LFTagsMapping'}, status=400)
