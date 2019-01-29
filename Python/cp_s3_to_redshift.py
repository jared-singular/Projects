#!/usr/bin/env python
import sys
import click
import atexit
import os
import logging
import re
import subprocess
from boto.s3.connection import S3Connection

from tools.bi_db_consts import RedShiftSpectrum
from tools.bi_db import DataB
from datetime import datetime as dt
from retrying import retry

# gobal declaration of redshift vars for db maintenance
rshift = None
vacuum_running = False
analyze_running = False

logger = logging.getLogger("sync_s3_redshift_mgr")
formatter = logging.Formatter('%(process)d|%(asctime)s|%(name)s|%(levelname)s|%(message)s')

error_handler = logging.FileHandler("error.log")
error_handler.setFormatter(formatter)
error_handler.setLevel(logging.ERROR)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)

logger.addHandler(stream_handler)
logger.addHandler(error_handler)
logger.setLevel(logging.DEBUG)


def vacuum_db():
    global vacuum_running
    if not vacuum_running:
        vacuum_running = True
        logger.info('Running: {}'.format('fn - vacuum_db'))
        rshift_4_vacuum = DataB(db_choice=RedShiftSpectrum, sql_language=RedShiftSpectrum.LANGUAGE)
        rshift_4_vacuum.create_conn()
        rshift_4_vacuum.conn.execute("""END TRANSACTION; VACUUM pub_master""")
        rshift_4_vacuum.kill_conn()
        vacuum_running = False


def re_analyze_db():
    global analyze_running
    if not analyze_running:
        analyze_running = True
        logger.info('Running: {}'.format('fn - re_analyze_db'))
        re_analyze_db = DataB(db_choice=RedShiftSpectrum, sql_language=RedShiftSpectrum.LANGUAGE)
        re_analyze_db.create_conn()
        re_analyze_db.conn.execute("""END TRANSACTION; ANALYZE pub_master""")
        re_analyze_db.kill_conn()
        analyze_running = False


def create_conn():
    logger.info('Running: {}'.format('fn - create_conn'))
    global rshift
    rshift = DataB(db_choice=RedShiftSpectrum, sql_language=RedShiftSpectrum.LANGUAGE)
    rshift.create_conn()
    return rshift


def get_csvs_currently_in_s3():
    logger.info('Running: {}'.format('fn - get_csvs_currently_in_s3'))
    s3_dir = subprocess.check_output("s3cmd ls --recursive s3://temp/".split(" "))
    csvs = []
    for x in s3_dir.split("\n"):
        if len(x) == 0:
            continue
        csvs.append(re.match(".*s3://.*date=(?P<date>[^/]+)/(?P<org_name>.*)", x).groups())
    return csvs


def close_all_open_db_connections():
    logger.info('Running: {}'.format('fn - close_all_open_db_connections'))
    if rshift:
        rshift.kill_all_db()


def get_date_org_list_from_redshift():
    logger.info('Running: {}'.format('fn - get_date_org_list_from_redshift'))
    # Pulls list [date, org] that exist in redshift
    create_conn()
    rshift_select = """
                    select start_date, org_name, inserted_at
                    from pub_master
                    group by 1, 2, 3
                    order by start_date desc
                    """
    result = rshift.conn.execute(rshift_select)
    return result.fetchall()


def get_org_and_date_to_copy_from_s3_to_redshift(csvs_in_s3, org_dict, org_meta_data, process_by_date):
    logger.info('Running: {}'.format('fn - get_org_and_date_to_copy_from_s3_to_redshift'))
    conn = S3Connection()
    bucket = conn.get_bucket('30d-retention-us-west-2')
    list_of_org_date_dicts_to_instruct_copy_from_s3_to_redshift = []

    if process_by_date:
        logger.debug('Dates that will be updated in bulk: {}'.format(org_dict.keys()))
        return org_dict.keys()

    for csv in csvs_in_s3:
        try:
            key_meta = bucket.get_key('temp/date={}/{}'.format(csv[0], csv[1]))
            most_recent_uploaded_to_s3 = dt.strptime(key_meta.last_modified, '%a, %d %b %Y %H:%M:%S %Z')
            previous_upload_to_s3 = org_meta_data[csv[1]]['previous_upload_to_s3']

            if csv[1] not in org_dict.get(csv[0], []) or ((csv[0] == org_meta_data[csv[1]]['date']) and (
                most_recent_uploaded_to_s3 - previous_upload_to_s3).days > 5
            ) or (csv[0] != org_meta_data[csv[1]]['date']):

                key_meta = bucket.get_key('temp/date={}/{}'.format(csv[0], csv[1]))
                if key_meta.size:
                    list_of_org_date_dicts_to_instruct_copy_from_s3_to_redshift.append({
                        'org_name': csv[1],
                        'date': csv[0]
                    })
                    # logger.debug('Updated Needed on {} for {}{}'.format(csv[0],
                    #    '{} b/c data was not found in Redshift but s3 file found w/ {}kb of data'.format(
                    #        csv[1], key_meta.size) if bool(csv[1] not in org_dict.get(csv[0], [])) else '',
                    #    '{} b/c data was refreshed {} days ago'.format(
                    #        csv[1], (most_recent_uploaded_to_s3 - previous_upload_to_s3).days) if bool((most_recent_uploaded_to_s3 - previous_upload_to_s3).days > 5) else ''))
        except Exception:
            # these are the dates that are not in org_dict as keys
            pass
    return list_of_org_date_dicts_to_instruct_copy_from_s3_to_redshift


def maintain_db_health(counter):
    logger.info('Running: {}'.format('fn - maintain_db_health'))
    DataB.kill_all_db()
    logger.debug('Ended all connections, starting vacumm')
    vacuum_db()
    logger.debug('Vacuum completed, starting db analyze')
    re_analyze_db()
    logger.debug('Finished db analyze command, creating new conn')
    global rshift
    rshift = create_conn()
    logger.debug('Continuing redshift work')


def drop_staging():
    logger.info('Running: {}'.format('fn - drop_staging'))
    rshift.conn.execute("""DROP TABLE IF EXISTS pub_staging;""")


def create_temp_staging():
    logger.info('Running: {}'.format('fn - create_temp_staging'))
    rshift.conn.execute("""
        CREATE TEMP TABLE pub_staging (LIKE pub_master);
    """)


def copy_from_s3_to_redshift(date, org_name=None):
    logger.info('Running: {}'.format('fn - copy_from_s3_to_redshift'))
    if org_name:
        s3_url = 's3://temp/date={}/{}'.format(date, org_name)
    else:
        s3_url = 's3://temp/date={}'.format(date)
    copy_command = """
        COPY pub_staging (start_date, org_id, org_name, inserted_at, org_partner_cost,
            organization_cost, uan_cost, app, source, os, platform, country_field, adn_sub_campaign_name,
            adn_sub_adnetwork_name, adn_original_currency, adn_campaign_name, keyword, publisher_id, publisher_site_name,
            unified_campaign_name, organization_currency, adn_cost, adn_impressions, custom_clicks,
            custom_installs, adn_original_cost, adn_clicks, adn_installs, revenue_1, revenue_1_original,
            revenue_7, revenue_7_original, revenue_14, revenue_14_original, revenue_30, revenue_30_original)
        FROM '{}'
        IAM_ROLE '{}'
        CSV IGNOREHEADER 1
        TIMEFORMAT AS 'YYYY-MM-DD HH24:MI:SS'
        MAXERROR AS 10;
    """.format(
        s3_url,
        os.getenv('ROLE'))

    rshift.conn.execute(copy_command)


def delete_from_redshift_where_updates_are_present():
    logger.info('Running: {}'.format('fn - delete_from_redshift_where_updates_are_present'))
    rshift.conn.execute("""BEGIN TRANSACTION;""")
    rshift.conn.execute("""
        DELETE FROM pub_master
            USING pub_staging
        WHERE pub_master.start_date = pub_staging.start_date
            AND pub_master.org_id = pub_staging.org_id
            AND pub_master.org_name = pub_staging.org_name
    """)


def insert_into_redshift_from_staging():
    logger.info('Running: {}'.format('fn - insert_into_redshift_from_staging'))
    rshift.conn.execute("""
        INSERT INTO pub_master
        SELECT start_date, org_id, org_name, inserted_at, org_partner_cost, organization_cost, uan_cost,
            app, source, os, platform, country_field, adn_sub_campaign_name, adn_sub_adnetwork_name,
            adn_original_currency, adn_campaign_name, keyword, publisher_id, publisher_site_name,
            unified_campaign_name, organization_currency, adn_cost, adn_impressions, custom_clicks,
            custom_installs, adn_original_cost, adn_clicks, adn_installs, revenue_1, revenue_1_original,
            revenue_7, revenue_7_original, revenue_14, revenue_14_original, revenue_30, revenue_30_original
        FROM pub_staging;
    """)
    rshift.conn.execute("""END TRANSACTION;""")


@retry(wait_exponential_multiplier=1000, wait_exponential_max=60000, stop_max_attempt_number=2)
def do_work(date, org_name=None):
    logger.info("Copying data from S3 to Redshift for: {}{}".format(
        org_name if org_name else '', ' - ' + date if org_name else date))
    trans = rshift.conn.begin()
    try:
        drop_staging()
        create_temp_staging()
        copy_from_s3_to_redshift(date, org_name)
        delete_from_redshift_where_updates_are_present()
        insert_into_redshift_from_staging()
        trans.commit()
        rshift.kill_all_db()
        rshift.create_conn()
        logger.info('Finished: {}{}, Moving on to next instruction'.format(
            org_name if org_name else '', ' - ' + date if org_name else date))
    except Exception, err:
        logger.error("Rolling back: {}".format(err))
        trans.rollback()
        logger.error("Writing to errors file: {}".format(err))
        rshift.kill_all_db()
        rshift.create_conn()
    except KeyboardInterrupt:
        sys.exit()


@click.group()
def cli():
    pass


@cli.command()
@click.option('--process-by', type=click.Choice(['date', 'dateorg']), default='date')
def copy_s3_data_to_redshift(process_by):
    logger.info('Running: {}'.format('fn - main'))
    process_by_date = True if process_by == 'date' else False
    csvs_in_s3 = get_csvs_currently_in_s3()

    data_from_redshift = get_date_org_list_from_redshift()

    redshift_orgs_organized_by_dict_key_date = {}
    org_meta_data = {}
    for date_org_tup in data_from_redshift:
        org_date = dt.strftime(date_org_tup[0], '%Y-%m-%d')
        org_name = date_org_tup[1]
        org_meta_data.update({
            date_org_tup[1]: {
                'date': dt.strftime(date_org_tup[0], '%Y-%m-%d'),
                'previous_upload_to_s3': date_org_tup[2]
            }
        })

        if org_date in redshift_orgs_organized_by_dict_key_date.keys():
            redshift_orgs_organized_by_dict_key_date[org_date].append(org_name)
        else:
            redshift_orgs_organized_by_dict_key_date.update({
                org_date: [org_name]
            })

    list_of_org_date_dicts_to_instruct_copy_from_s3_to_redshift = get_org_and_date_to_copy_from_s3_to_redshift(
        csvs_in_s3, redshift_orgs_organized_by_dict_key_date, org_meta_data, process_by_date
    )

    counter = 0
    for instruction in list_of_org_date_dicts_to_instruct_copy_from_s3_to_redshift:
        date = instruction if process_by_date else instruction['date']
        name = None if process_by_date else instruction['org_name']

        counter += 1
        if (counter % 30 == 0 or counter == 1):
            maintain_db_health(counter)

        do_work(date, name)


if __name__ == '__main__':
    atexit.register(close_all_open_db_connections)
    copy_s3_data_to_redshift()