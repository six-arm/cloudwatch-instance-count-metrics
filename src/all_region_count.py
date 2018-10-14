#!/usr/bin/env python3.6

import boto3
import collections
from pprint import pformat
import dateutil.parser
import os
import re
import logging

logging.basicConfig()
log = logging.getLogger('res_counts')
log.setLevel(logging.INFO)


ReservationType = collections.namedtuple('ReservationType', ['size', 'location', 'tenancy', 'product'])
Instance = collections.namedtuple('Instance', ['type', 'status'])


ec2 = boto3.client('ec2')
cloudwatch = boto3.client('cloudwatch')


REGION_NAME                           = boto3._get_default_session().region_name
METRIC_NAMESPACE                      = os.environ.get('METRIC_NAMESPACE', 'Trackit')
METRIC_NAME_INSTANCES                 = os.environ.get('METRIC_NAME_INSTANCES', 'Instance count')
DEBUG                                 = False


_az_to_region_re = re.compile(r'^(.+?)[a-z]?$')
def _az_to_region(az):
    return _az_to_region_re.match(az).group(1)
    

def _avail_regions():
    regions = ec2.describe_regions()
    for region in regions.get('Regions'):
        log.debug("Regions: {}".format(region.get('RegionName')))
    return regions.get('Regions')

def _get_instances(region):
    ec2 = boto3.client('ec2', region_name=region)
    instance_paginator = ec2.get_paginator('describe_instances')
    return sorted(
        (
            Instance(
                type=ReservationType(
                    size=instance['InstanceType'],
                    location=instance['Placement']['AvailabilityZone'],
                    tenancy=instance['Placement']['Tenancy'],
                    product=instance.get('Platform', 'Linux/UNIX'),
                ),
                status=instance['State']['Name'],
            )
            #for page in instance_paginator.paginate(Filters=[{'Name': 'instance-state-name', 'Values': ['pending', 'running']}])
            for page in instance_paginator.paginate()
            for reservation in page['Reservations']
            for instance in reservation['Instances']
            #if instance.get('InstanceLifecycle', 'ondemand') == 'ondemand'
        ),
        key=lambda instance: instance.type
    )

def _aggregated_instances(instances):
    agg = collections.defaultdict(int)
    for instance in instances:
        agg[instance.type] += 1
    return [
        (type, count)
        for type, count in agg.items()
    ]

def _make_instances_metric_data(now, instances):
    return [
        {
            'MetricName': METRIC_NAME_INSTANCES,
            'Timestamp': now,
            'Value': count,
            'Unit': 'Count',
            'Dimensions': [
                { 'Name': 'InstanceType', 'Value': instance_type.size },
                { 'Name': 'Region'      , 'Value': _az_to_region(instance_type.location) },
                { 'Name': 'Location'    , 'Value': instance_type.location },
                { 'Name': 'Tenancy'     , 'Value': instance_type.tenancy },
                { 'Name': 'Product'     , 'Value': instance_type.product },
            ],
        }
        for instance_type, count in _aggregated_instances(instances)
    ]


def _instance_matches_reserved_instance(instance_type, reserved_instance_type):
    return (
        reserved_instance_type.size     == instance_type.size     and
        reserved_instance_type.location in instance_type.location and
        reserved_instance_type.tenancy  == instance_type.tenancy  and
        reserved_instance_type.product  == instance_type.product
    )


def next_or_none(iterator):
    try:
        return next(iterator)
    except StopIteration:
        return None


def _put_metrics(metric_data):
    if DEBUG:
        for data in metric_data:
            log.debug("MD: {}".format(pformat(data)))
    else:
        if metric_data:
            cloudwatch.put_metric_data(
                Namespace=METRIC_NAMESPACE,
                MetricData=metric_data,
            )


def lambda_handler(event, context):
    now = dateutil.parser.parse(event['time'])
    for region in _avail_regions():
        instances = _get_instances(region.get('RegionName', ['us-west-2']))
        instances_metric_data = _make_instances_metric_data(now, instances)
        count = 0
        for md in instances_metric_data:
            count += md.get('Value')
        log.info("{} total instances: {}".format(region.get('RegionName'), count))
        _put_metrics(instances_metric_data)


if __name__ == '__main__':
    import datetime
    DEBUG = True
    if DEBUG:
        log.setLevel(logging.DEBUG)
    now = datetime.datetime.now()
    lambda_handler({
        'time': now.isoformat(),
    }, None)
