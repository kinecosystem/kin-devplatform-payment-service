import os
import base64

import boto3

SSM_REGION = os.environ.get('SSM_REGION', 'us-east-1')
SEEDS_PARAMETER_NAME = os.environ('SEEDS_PARAMETER_NAME')


def get_seeds():
    # Get the seeds from aws ssm, decrypt with the aws kms key

    """
    The seeds are saved on aws ssm, encrypted with a key from aws kms
    The value is a base64encoded text in this form:
    appId:seed
    """

    # Get value
    client = boto3.client('ssm', region_name=SSM_REGION)
    seed_parameter = client.get_parameter(Name=SEEDS_PARAMETER_NAME, WithDecryption=True)
    parameter_value = seed_parameter['Parameter']['Value']
    # Decode value
    decoded_value = base64.b64decode(parameter_value).decode()
    # Create dict of app_id:seed
    seed_list = decoded_value.splitlines()
    APP_SEEDS = {}
    for line in seed_list:
        seed_pair = line.split(':')
        seed_keys = seed_pair[1].split(',')
        APP_SEEDS[seed_pair[0]] = DS_Wallets((seed_keys[0], seed_keys[1]))

    return APP_SEEDS


class DS_Wallets:
    def __init__(self, ds_our, ds_joined):
        self.our = ds_our
        self.joined = ds_joined