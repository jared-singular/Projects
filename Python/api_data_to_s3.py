#!/usr/bin/env python
import collections
import urlparse
import datetime
import boto3
import requests
import logging
import sys
import os
import click
import time
import csv
import cStringIO
import gevent
import psutil
import gc

from retrying import retry
from collections import namedtuple
from gevent.queue import Queue
from gevent.monkey import patch_all

from bi_db import bi_db_connection

from singular_api_client.singular_client import SingularClient
from singular_api_client.params import (Metrics, DiscrepancyMetrics, Format,
                                        CountryCodeFormat, Dimensions,
                                        TimeBreakdown)
from singular_api_client.exceptions import retry_if_unexpected_error

patch_all()

WHITE = "\033[0m"
PURPLE = "\033[0;35m"
RED = "\033[0;31m"
GREEN = "\033[0;32m"
YELLOW = "\033[0;33m"
BLUE = "\033[0;34m"
CYAN = "\033[0;36m"
RED_UNDERLINE = "\033[4;31m"
GREEN_UNDERLINE = "\033[4;32m"
YELLOW_UNDERLINE = "\033[4;33m"
BLUE_UNDERLINE = "\033[4;34m"
PURPLE_UNDERLINE = "\033[4;35m"
CYAN_UNDERLINE = "\033[4;36m"
WHITE_UNDERLINE = "\033[4;37m"

FIVE_MINUTES = 60 * 5
DATE_FORMAT = "%Y-%m-%d"
DEFAULT_START_DATE = datetime.datetime.strftime(
    datetime.datetime.today() - datetime.timedelta(days=32), "%Y-%m-%d")
DEFAULT_END_DATE = datetime.datetime.strftime(
    datetime.datetime.today() - datetime.timedelta(days=30), "%Y-%m-%d")

logger = logging.getLogger("etl_manager")
logger2 = logging.getLogger("client")

formatter = logging.Formatter('\
    [%(process)d :: %(thread)d] - %(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler("sync.log")
file_handler.setFormatter(formatter)

error_handler = logging.FileHandler("error.log")
error_handler.setFormatter(formatter)
error_handler.setLevel(logging.ERROR)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

for cur_logger in [logger, logger2]:
    cur_logger.setLevel(logging.DEBUG)
    cur_logger.addHandler(file_handler)
    cur_logger.addHandler(error_handler)


DEFAULT_DIMENSIONS = [Dimensions.APP, Dimensions.SOURCE, Dimensions.OS, Dimensions.PLATFORM,
                      Dimensions.COUNTRY_FIELD, Dimensions.ADN_SUB_CAMPAIGN_NAME,
                      Dimensions.ADN_SUB_ADNETWORK_NAME, Dimensions.ADN_ORIGINAL_CURRENCY,
                      Dimensions.ADN_CAMPAIGN_NAME
                      ]

DEFAULT_PUBLISHER_DIMENSIONS = [Dimensions.KEYWORD, Dimensions.PUBLISHER_ID,
                                Dimensions.PUBLISHER_SITE_NAME
                                ]

DEFAULT_CREATIVE_DIMENSIONS = [Dimensions.ADN_CREATIVE_NAME, Dimensions.ADN_CREATIVE_ID,
                               Dimensions.SINGULAR_CREATIVE_ID, Dimensions.CREATIVE_HASH,
                               Dimensions.CREATIVE_IMAGE_HASH, Dimensions.CREATIVE_IMAGE,
                               Dimensions.CREATIVE_WIDTH, Dimensions.CREATIVE_HEIGHT,
                               Dimensions.CREATIVE_IS_VIDEO, Dimensions.CREATIVE_TEXT,
                               Dimensions.CREATIVE_URL, Dimensions.KEYWORD
                               ]

DEFAULT_METRICS = [Metrics.ADN_COST, Metrics.ADN_IMPRESSIONS, Metrics.CUSTOM_CLICKS,
                   Metrics.CUSTOM_INSTALLS, Metrics.ADN_ORIGINAL_COST
                   ]

DEFAULT_DISCREPANCY_METRICS = [DiscrepancyMetrics.ADN_CLICKS, DiscrepancyMetrics.ADN_INSTALLS
                               ]

DEFAULT_COHORT_METRICS = ['revenue']  # create new table w/ addl cohort fields
DEFAULT_COHORT_PERIODS = [1, 7, 14, 30]

APIKey = namedtuple("APIKey", ["org_name", "api_key", "org_id"])


def memory_usage_psutil():
    # return the memory usage in MB
    return psutil.Process(os.getpid()).memory_info()[0] / float(2 ** 20)


def daterange(start_date, end_date):
    cur_date = start_date
    while cur_date <= end_date:
        yield cur_date
        cur_date = cur_date + datetime.timedelta(days=1)


def encode_date(date_obj):
    return date_obj.strftime(DATE_FORMAT)


class RedshiftSync(object):
    MAX_REPORTS_TO_QUEUE = 1
    DEFAULT_UPDATE_WINDOW_DAYS = 30
    REFRESH_TIMEOUT = 60 * 60 * 6  # 6 HOURS

    def __init__(self, s3_path, api_keys, dimensions=DEFAULT_DIMENSIONS,
                 metrics=DEFAULT_METRICS, cohort_metrics=DEFAULT_COHORT_METRICS,
                 cohort_periods=DEFAULT_COHORT_PERIODS, display_alignment=True,
                 format=Format.JSON, country_code_format=CountryCodeFormat.ISO3,
                 max_parallel_reports=MAX_REPORTS_TO_QUEUE,
                 discrepancy_metrics=DEFAULT_DISCREPANCY_METRICS):

        self.max_parallel_reports = max_parallel_reports
        self.api_keys = api_keys
        self.dimensions = dimensions
        self.metrics = metrics
        self.discrepancy_metrics = discrepancy_metrics
        self.cohort_metrics = cohort_metrics
        self.cohort_periods = cohort_periods
        self.display_alignment = display_alignment
        self.format = format
        self.country_code_format = country_code_format
        self.exchange_rate = {}

        self.columns = ["start_date", "org_id", "org_name", "inserted_at", "org_partner_cost",
                        "organization_cost", "uan_cost"] + \
            self.dimensions + self.metrics + self.discrepancy_metrics
        for period in self.cohort_periods:
            self.columns += ["{}_{}".format(cohort, period) for cohort in self.cohort_metrics]
            self.columns += ["{}_{}_{}".format(cohort, period, "original"
                                               ) for cohort in self.cohort_metrics]

        parsed_s3_path = urlparse.urlparse(s3_path)
        self.bucket = parsed_s3_path.netloc
        self.s3_prefix = parsed_s3_path.path
        self.s3_path = s3_path

    def download_to_s3(self, start_date, end_date):
        days = list(daterange(start_date, end_date))
        logger.info("Processing {} days of data".format(len(days)))

        for day in days:

            logger.info("Adding %d reports to the queue" % len(self.api_keys))
            tasks = Queue()
            for x in self.api_keys:
                if x.api_key:
                    tasks.put_nowait((x, day.strftime(DATE_FORMAT)))

            # This statement will not continue until all workers are done/cancelled/killed
            gevent.joinall([gevent.spawn(self.run_async_report_worker, i, tasks)
                            for i in xrange(self.max_parallel_reports)])

            logger.info("Finished all queries for {}".format(day.strftime(DATE_FORMAT)))

    def run_async_report_worker(self, worker_id, tasks):
        while not tasks.empty():
            self.run_async_report(*tasks.get())

    def run_async_report(self, api_key, date):
        try:
            logger.info("Pulling data for {}{}{}, date={}".format(
                YELLOW_UNDERLINE, api_key.org_name, WHITE, date))
            # time.sleep(3)
            self.run_async_report_inner(api_key, date)
        except Exception as e:
            logger.error("Exception while pulling data for {}, date={}: {}".format(
                api_key.org_name, date, e))

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=60000,
           retry_on_exception=retry_if_unexpected_error, stop_max_attempt_number=5)
    def run_async_report_inner(self, api_key, date):
        """
        Enqueue and Poll one report from Singular API (using the async endpoint)
        Note: This method doesn't have any return value and should raise an
              Exception in case of failure

        :param api_key: org api_key
        :param date: requested date formatted in "%Y-%m-%d"
        """
        client = SingularClient(api_key.api_key)

        client.BASE_API_URL = ""

        report_id = client.create_async_report(
            start_date=date,
            end_date=date,
            format=self.format,
            dimensions=self.dimensions,
            metrics=self.metrics,
            discrepancy_metrics=self.discrepancy_metrics,
            cohort_metrics=self.cohort_metrics,
            cohort_periods=[str(x) for x in self.cohort_periods],
            time_breakdown=TimeBreakdown.DAY,
            country_code_format=self.country_code_format,
            display_alignment=self.display_alignment
        )

        while True:
            report_status = client.get_report_status(report_id)
            if report_status.status not in [report_status.QUEUED, report_status.STARTED]:
                break
            time.sleep(10)
        if report_status.status != report_status.DONE:
            logger.error("async report failed -- org_name = %s, date = %s: %s" %
                         (api_key.org_name, date, repr(report_status)))
            return

        # Process the new report
        logger.info("async report finished successfully -- org_name = %s%s%s, date = %s: %s" %
                    (GREEN_UNDERLINE, api_key.org_name, WHITE, date, repr(report_status)))

        self.handle_report_data(api_key, date, report_status.download_url, report_id)

    def handle_report_data(self, api_key, date, download_url, report_id):

        # TODO: To conserve more memory, move from downloading everything to streaming...
        # Download result
        res = requests.get(download_url)
        if not res.ok:
            logger.error("unexpected error when downloading org_name = %s, report_id=%s, url=%s" %
                         (api_key.org_name, report_id, download_url))
            return

        membuf = cStringIO.StringIO()
        result_data = res.json()['results']

        # Our API returns the CSV output with a dictionary in the cohorted fields,
        # so we have to download and transform

        result_data = self.convert_currencies(result_data)
        result_data = self.remove_nas(result_data)
        if len(result_data) > 0:
            for row in result_data:
                del row["end_date"]
                row.update({
                    "org_id": api_key.org_id,
                    "org_name": api_key.org_name,
                    "inserted_at": datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                })
            dw = csv.DictWriter(membuf, self.columns, restval='', doublequote=1,
                                quoting=csv.QUOTE_ALL)
            dw.writeheader()
            dw.writerows(RedshiftSync.convert(result_data))

        # Upload to S3
        key = os.path.join(self.s3_prefix, "date=%s" % date, api_key.org_name)
        if key.startswith("/"):
            key = key[1:]
        boto3.resource("s3").Bucket(self.bucket).put_object(Key=key, Body=membuf.getvalue(),
                                                            ServerSideEncryption='aws:kms')

        logger.info("finished uploading report data to: %s" % os.path.join(self.bucket, key))
        logger.info("MEMORY CONSUMPTION: {}{}{}".format(RED_UNDERLINE, memory_usage_psutil(),
                                                        WHITE))
        gc.collect()

    def remove_nas(self, data):

        for row in data:
            cols_labeled_na = [
                'creative_hash', 'creative_image_hash', 'creative_image', 'creative_text',
                'creative_url', 'creative_width', 'creative_height', 'creative_is_video',
                'adn_creative_name', 'singular_creative_id', 'adn_creative_id',
                'adn_sub_campaign_name', 'adn_sub_adnetwork_name', 'adn_campaign_name', 'keyword',
                'publisher_id', 'publisher_site_name', 'unified_campaign_name', 'country_field'
            ]
            cols_labeled_unknown = [
                'unified_campaign_name'
            ]
            cols_to_fill_zero = [
                'tracker_installs', 'tracker_reengagements', 'adn_impressions', 'adn_clicks',
                'custom_clicks', 'custom_installs', 'adn_installs', 'revenue_1',
                'revenue_1_original', 'revenue_7', 'revenue_7_original', 'revenue_14',
                'revenue_14_original', 'revenue_30', 'revenue_30_original', 'retention_rate_1',
                'retention_rate_7', 'retention_rate_14', 'retention_rate_30'
            ]

            for cur_col in list(set(self.columns) & set(cols_labeled_na)):
                row[cur_col] = None if row.get(cur_col) == 'N/A' else row.get(cur_col)
                row[cur_col] = None if row.get(cur_col) == '' else row.get(cur_col)

            for cur_col in list(set(self.columns) & set(cols_labeled_unknown)):
                row[cur_col] = None if row.get(cur_col) == 'unknown' else row.get(cur_col)

            for cur_col in list(set(self.columns) & set(cols_to_fill_zero)):
                row[cur_col] = row.get(cur_col) if row.get(cur_col) else 0

        return data

    def convert_currencies(self, data):
        query_result = None
        uan_currency = None
        organization_currency = None
        currency_query = """
            select c1.factor, c2.code
            from currencies_dailycurrencyexchangerate c1
            left join currencies_currency c2 on c1.currency_id = c2.id
            where c1.date = '{}'
            and c2.code = '{}'
        """
        for ii, row in enumerate(data):
            start_date = row['start_date']
            row['adn_original_currency'] = row['adn_original_currency'] if (
                row.get('adn_original_currency') and 'N/A' not in row.get('adn_original_currency')
            ) else 'USD'

            row['organization_currency'] = row['organization_currency'] if \
                row.get('organization_currency') else 'USD'

            # cost transforming for Org
            if row['organization_currency'] != organization_currency:
                query_result = RedshiftSync.query_mobcnc(currency_query.format(
                    start_date, row['organization_currency']))
                if isinstance(query_result, list) and (
                        query_result[0].get('code', {}) == row['organization_currency']):

                    organization_currency = query_result[0]['code']

                    self.exchange_rate[row['organization_currency']] = float(
                        query_result[0].get('factor', 1))
                else:
                    logger.exception('organization currency not found in mobcnc')

            row['organization_cost'] = 0
            if row['adn_cost'] > 0:
                row['organization_cost'] = row['adn_cost'] / self.exchange_rate[
                    row['organization_currency']]
            elif row['adn_cost'] == 'N/A' or not row['adn_cost'] or row['adn_cost'] < 0:
                row['adn_cost'] = 0
            else:
                import ipdb
                ipdb.set_trace()
                print

            if not self.exchange_rate.get(row['adn_original_currency']) and \
                    row['adn_original_currency'] != uan_currency:
                query_result = RedshiftSync.query_mobcnc(currency_query.format(
                    start_date, row['adn_original_currency']))
                if isinstance(query_result, list) and (
                        query_result[0].get('code', {}) == row['adn_original_currency']):
                    uan_currency = query_result[0]['code']
                    self.exchange_rate[row['adn_original_currency']] = float(
                        query_result[0].get('factor', 1))
                else:
                    logger.exception('uan currency code not found in mobcnc')

            row['uan_cost'] = 0
            if row['adn_original_cost'] > 0:
                row['uan_cost'] = row['adn_original_cost'] / self.exchange_rate[
                    row['adn_original_currency']]
            elif row['adn_original_cost'] == 'N/A' or not \
                    row['adn_original_cost'] or row['adn_original_cost'] < 0:
                row['adn_original_cost'] = 0

            row['org_partner_cost'] = 0
            if row['organization_cost'] and row['uan_cost'] > 0:
                row['org_partner_cost'] = row['organization_cost'] - row['uan_cost']
                if row['org_partner_cost'] < 0:
                    row['org_partner_cost'] = 0

            # revenue transforming
            for cohort in self.cohort_metrics:
                for period in self.cohort_periods:
                    if (row[cohort].get('{}d'.format(period), None) and (
                            row['organization_currency'] != 'USD')):
                        row["{}_{}".format(cohort, period)] = (
                            row[cohort].get('{}d'.format(period)) /
                            self.exchange_rate[row['organization_currency']])
                    else:
                        row["{}_{}".format(cohort, period)] = \
                            row[cohort].get('{}d'.format(period), None)
                    row["{}_{}_{}".format(cohort, period, "original")] = \
                        row[cohort].get('{}d'.format(period), None)
                del row[cohort]

        return data

    @staticmethod
    def convert(data):
        if isinstance(data, basestring):
            return data.encode('utf-8')
        elif isinstance(data, collections.Mapping):
            return dict(map(RedshiftSync.convert, data.iteritems()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(RedshiftSync.convert, data))
        else:
            return data

    @staticmethod
    def query_mobcnc(query):
        connection = bi_db_connection(
            db_host=os.getenv("BI_HOST"),
            db_port=int(os.getenv("BI_PORT")),
            db_user=os.getenv("BI_USER"),
            db_pass=os.getenv("BI_PASSWORD"),
            db_name=os.getenv("BI_NAME"))
        return connection.execute(query)


@click.group()
def cli():
    pass


def filter_api_keys(api_keys, cherry_pick, exclude_org_name, org_name):
    api_keys_filtering = []

    if cherry_pick:
        for ix, key in enumerate(api_keys):
            print("[{0}] - {1}".format(ix + 1, key.org_name, ))
        value = click.prompt('Enter all org indices you want to run \
            script for (comma delimited)')
        values = [int(idx) - 1 for idx in value.split(',')]
        for picked_org in values:
            api_keys_filtering.append(api_keys[picked_org])

    else:
        [[api_keys_filtering.append(org)
            for org in api_keys if name == org.org_name] for name in org_name]
        [[api_keys_filtering.append(org)
            for org in api_keys if name != org.org_name] for name in exclude_org_name]

    api_keys = api_keys_filtering if api_keys_filtering else api_keys
    return api_keys


@cli.command()
@click.argument("s3_bucket")
@click.option('--start-date', '-s', default=DEFAULT_START_DATE, help="default is 31 days ago")
@click.option('--extra-dimensions', '-d', multiple=True, help="additional dimensions to pull")
@click.option('--end-date', '-e', default=DEFAULT_END_DATE, help="default is 30 days ago")
@click.option('--max-parallel-reports', '-m', default=5, help="# of reports running in parallel")
@click.option('--org-name', '-o', multiple=True, default=[], help="run script for specific orgs")
@click.option('--exclude-org-name', '-x', multiple=True, default=[], help="ignore specific orgs")
@click.option('--cherry-pick', '-c', is_flag=True, help="Pick from list of orgs with indexs")
@click.option('--report-type', type=click.Choice(
    ['publisher', 'creative', 'generic']), default='generic')
def download_to_s3(s3_bucket, start_date, end_date, extra_dimensions,
                   max_parallel_reports, org_name, exclude_org_name, cherry_pick, report_type):
    query = """
        select o.name as org_name, ou.api_key, o.id as org_id
        from dashboard_organization o
        left join dashboard_organizationuser ou on o.id = ou.organization_id
        where ou.is_active = 1
        and o.name not like '%%_dev'
        and o.name not like '%%test%%'
        and o.name not like '%%exercise%%'
        and o.name not like '%%training%%'
        and o.name not like '%%mock%%'
        and o.name not like '%%demo%%'
        and o.name not like '%%(copy)%%'
        and o.account_type = 1
        and company_logo is not null
        and company_logo <> ''
        and min_stats_date is not null
        group by o.name;
    """
    # Get relevant orgs and API keys
    raw_api_keys = [APIKey(**kwargs) for kwargs in RedshiftSync.query_mobcnc(query)]
    api_keys = filter_api_keys(raw_api_keys, cherry_pick, exclude_org_name, org_name)

    # Start/End date
    start_date = datetime.datetime.strptime(start_date, DATE_FORMAT).date()
    end_date = datetime.datetime.strptime(end_date, DATE_FORMAT).date()

    # Define dimensions
    dimensions = DEFAULT_DIMENSIONS
    if report_type == 'publisher':
        dimensions += DEFAULT_PUBLISHER_DIMENSIONS
    elif report_type == 'creative':
        dimensions += DEFAULT_CREATIVE_DIMENSIONS
    dimensions += [x for x in extra_dimensions if x not in dimensions]

    # Download data
    RedshiftSync(s3_bucket, api_keys, dimensions=dimensions,
                 max_parallel_reports=max_parallel_reports) \
        .download_to_s3(start_date, end_date)


@cli.command()
@click.argument("s3_bucket")
@click.option('--start-date', '-s', default=DEFAULT_START_DATE, help="default is 31 days ago")
@click.option('--extra-dimensions', '-d', multiple=True, help="additional dimensions to pull")
@click.option('--end-date', '-e', default=DEFAULT_END_DATE, help="default is 30 days ago")
@click.option('--max-parallel-reports', '-m', default=5, help="# of reports running in parallel")
@click.option('--report-type', type=click.Choice(
    ['publisher', 'creative', 'generic']), default='generic')
def complete_missed_orgs(s3_bucket, start_date, end_date, extra_dimensions,
                         max_parallel_reports, report_type):
    query = """
        select o.name as org_name, ou.api_key, o.id as org_id
        from dashboard_organization o
        left join dashboard_organizationuser ou on o.id = ou.organization_id
        where ou.is_active = 1
        and o.name not like '%%_dev'
        and o.name not like '%%test%%'
        and o.name not like '%%exercise%%'
        and o.name not like '%%training%%'
        and o.name not like '%%mock%%'
        and o.name not like '%%demo%%'
        and o.name not like '%%(copy)%%'
        and o.account_type = 1
        and company_logo is not null
        and company_logo <> ''
        and min_stats_date is not null
        group by o.name;
    """
    # Get relevant orgs and API keys
    api_keys = [APIKey(**kwargs) for kwargs in RedshiftSync.query_mobcnc(query)]

    # Start/End date
    start_date = datetime.datetime.strptime(start_date, DATE_FORMAT).date()
    end_date = datetime.datetime.strptime(end_date, DATE_FORMAT).date()

    # Define dimensions
    dimensions = DEFAULT_DIMENSIONS
    if report_type == 'publisher':
        dimensions += DEFAULT_PUBLISHER_DIMENSIONS
    elif report_type == 'creative':
        dimensions += DEFAULT_CREATIVE_DIMENSIONS
    dimensions += [x for x in extra_dimensions if x not in dimensions]

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(r"singular-store-30d-retention-us-west-2")
    for day in daterange(start_date, end_date):

        s3_path = "master_api/temp/date={}".format(day.strftime(DATE_FORMAT))
        existing_orgs = [os.path.split(x.key)[-1]
                         for x in bucket.objects.filter(Prefix=s3_path)]
        keys_for_date = [x for x in api_keys if x.org_name not in existing_orgs]

        if len(keys_for_date) == 0:
            continue

        logger.info("date={}, missing orgs: {}".format(day, keys_for_date))
        # Download data
        RedshiftSync(s3_bucket, keys_for_date, dimensions=dimensions,
                     max_parallel_reports=max_parallel_reports) \
            .download_to_s3(day, day)


if __name__ == "__main__":
    cli()