import os
import base64

import boto3

IS_LOCAL = os.environ.get('IS_LOCAL', 'false')
SSM_REGION = os.environ.get('SSM_REGION', 'us-east-1')
SEEDS_PARAMETER_NAME = os.environ.get('SEEDS_PARAMETER_NAME')


def get_seeds():
    # Get the seeds from aws ssm, decrypt with the aws kms key

    """
    The seeds are saved on aws ssm, encrypted with a key from aws kms
    The value is a base64encoded text in this form:
    appId:our_seed,joined_seed
    """

    if IS_LOCAL == 'true':
        local_test_seed = os.environ.get('LOCAL_TEST_SEED', '')
        return {'test': DS_Wallets(local_test_seed,local_test_seed)}
    # Get value    
    client = boto3.client('ssm', region_name=SSM_REGION)
    seed_list = []
    # The value was too big, so we needed to split it to multiple parameters
    for name in SEEDS_PARAMETER_NAME.split(','):
        seed_parameter = client.get_parameter(Name=name, WithDecryption=True)
        parameter_value = seed_parameter['Parameter']['Value']
        # Decode value
        decoded_value = base64.b64decode(parameter_value).decode()
        seed_list.extend(decoded_value.splitlines())
    APP_SEEDS = {}
    for line in seed_list:
        seed_pair = line.split(':')
        seed_keys = seed_pair[1].split(',')
        APP_SEEDS[seed_pair[0]] = DS_Wallets(seed_keys[0], seed_keys[1])

    return APP_SEEDS


class DS_Wallets:
    def __init__(self, ds_our, ds_joined):
        self.our = ds_our
        self.joined = ds_joined