import argparse
import json
import logging
import pandas as pd
import requests
import hashlib
import sys
import numpy as np

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Define a function to hash a value
def hashing(key):
    # Convert the value to a string and encode it
    return hashlib.sha256(str(key).encode()).hexdigest()


def create_dataframe():
    columns = ['tenant_id', 'ui_origin', 'tenant_name', 'workspace', 'request_type',
               'trace', 'request_type_raw', 'view', 'report', 'actionButton', 'page',
               'request_id', 'email', 'total', 'webapi', 'redis',
               'mssql', 'timestamp']
    return pd.DataFrame(columns=columns)


def get_data(psr_envi, api_key, tenant_id, start_date, end_date, elk_url):
    # start_date += "T18:30:00.000Z"
    # end_date += "T18:00:00.000Z"
    url = f"https://{elk_url}/_search"
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': api_key
    }

    # Initialize variables for pagination
    all_hits = []
    search_after = None
    size = 1000  # Batch size per request
    has_more_data = True

    while has_more_data:
        # Build payload with search_after if present
        query = {
            "size": size,
            "query": {
                "bool": {
                    "must": [],
                    "filter": [
                        {
                            "bool": {
                                "filter": [
                                    {
                                        "bool": {
                                            "should": [
                                                {
                                                    "match_phrase": {
                                                        "environment.name": psr_envi
                                                    }
                                                }
                                            ],
                                            "minimum_should_match": 1
                                        }
                                    },
                                    {
                                        "bool": {
                                            "should": [
                                                {
                                                    "match": {
                                                        "request.tenant_id": tenant_id
                                                    }
                                                }
                                            ],
                                            "minimum_should_match": 1
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "range": {
                                "@timestamp": {
                                    "format": "strict_date_optional_time",
                                    "gte": start_date,
                                    "lte": end_date
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [
                {"@timestamp": "asc"},  # Sort by timestamp in ascending order
                {"_index": "asc"}  # Tie-breaker for duplicate timestamps
            ]
        }
        if search_after:
            query["search_after"] = search_after

        payload = json.dumps(query)
        response = requests.post(url, headers=headers, data=payload, verify=False)
        print(json.dumps(response.json(), indent=2))
        logger.info(f"Got {response.status_code} from Tenant")

        if response.status_code != 200:
            logger.error(f"Request failed with status code {response.status_code}")
            return None

        # Process the hits
        data = response.json()
        hits = data.get("hits", {}).get("hits", [])
        all_hits.extend(hits)

        # Check if we need to continue
        if len(hits) < size:
            has_more_data = False
        else:
            # Get the `sort` value of the last hit for the next query
            search_after = hits[-1].get("sort")

    # print(all_hits)
    return all_hits


def process_data(all_hits, df):
    if not all_hits:
        logger.warning("No data found in the response.")
        return pd.DataFrame(columns=['envi_name', 'envi_version', 'timestamp', 'tenant_id',
                                     'tenant_name', 'id', 'request_type', 'trace_id',
                                     'workspace', 'page', 'view', 'report', 'action_button',
                                     'user_email', 'ui_origin', 'timings_webapi',
                                     'timings_redis', 'timings_mssql', 'timings_total',
                                     'timings_mongodb', 'timings_hbase', 'timings_liveserver'])

    df_list = []
    for hit in all_hits:
        source = hit['_source']
        request = source.get('request', {})
        timings = source.get('timings', {})
        environment = source.get('environment', {})
        timestamp = source.get('@timestamp')

        combined = {**request, **timings, **environment, 'timestamp': timestamp}
        df_list.append(combined)

    return pd.DataFrame(df_list)


def save_to_csv(df, psr_envi, tenant_id):
    filename = 'ELKPSRData.csv'

    df = df.rename(columns={'name': 'envi_name', 'version': 'envi_version', 'hbase': 'timings_hbase',
                            'live server': 'timings_liveserver', 'total': 'timings_total', 'webapi': 'timings_webapi',
                            'redis': 'timings_redis', 'mssql': 'timings_mssql', 'mongodb': 'timings_mongodb'})
    df['timestamp'] = pd.to_datetime(df['timestamp']).apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    df = df[['envi_name', 'envi_version', 'timestamp', 'tenant_id', 'tenant_name', 'id', 'request_type', 'trace_id',
             'workspace', 'page', 'view', 'report', 'action_button', 'user_email', 'ui_origin', 'timings_webapi',
             'timings_redis', 'timings_mssql', 'timings_total', 'timings_mongodb', 'timings_hbase',
             'timings_liveserver']]

    df = df[(df['workspace'].notnull()) & (df['workspace'] != '') & (~df['user_email'].str.contains('o9solutions')) & (
        ~df['report'].str.contains('View')) & (~df['user_email'].str.contains('etluser'))]
    df["user_email"] = df["user_email"].apply(hashing)
    if df.empty:
        logger.warning("Saving empty dataframe.")
    df.to_csv(filename, sep='^', index=False, quoting=2)
    logger.info(f"Data saved to {filename}")


def main():
    try:

        parser = argparse.ArgumentParser(description="Process PSR data.")
        parser.add_argument('--PSR_Envi', '-env', type=str, help='PSR_Envi')
        parser.add_argument('--PSR_AuthToken', '-api', type=str, help='PSR_AuthToken')
        parser.add_argument('--PSR_StartDate', '-start', type=str, help='PSR_StartDate')
        parser.add_argument('--PSR_EndDate', '-end', type=str, help='PSR_EndDate')
        parser.add_argument('--Tenant_Id', '-tid', type=str, help='Tenant_Id')
        parser.add_argument('--PSR_ELK_URL', '-elkurl', type=str, help='ELK_URL')

        args = parser.parse_args()

        psr_envi = args.PSR_Envi
        api_key = args.PSR_AuthToken
        start_date = args.PSR_StartDate
        end_date = args.PSR_EndDate
        tenant_id = args.Tenant_Id
        elk_url = args.PSR_ELK_URL

        if not args.PSR_Envi:
            logging.error("PSR_Environment not specified : Exiting code...")
            sys.exit("Please provide a value for the PSR_Envi argument.")

        if not args.PSR_AuthToken:
            logging.error("API Key not specified : Exiting code...")
            sys.exit("Please provide a value for the PSR_AuthToken argument.")

        if not args.PSR_StartDate:
            logging.error("Start Date not specified : Exiting code...")
            sys.exit("Please provide a value for the PSR_StartDate Key argument.")
        if not args.PSR_EndDate:
            logging.error("End Date Key not specified : Exiting code...")
            sys.exit("Please provide a value for the PSR_EndDate Key argument.")
        if not args.Tenant_Id:
            logging.error("Tenant Id Key not specified : Exiting code...")
            sys.exit("Please provide a value for the Tenant_Id Key argument.")
        if not args.PSR_ELK_URL:
            logging.error("PSR_ELK_URL Key not specified : Exiting code...")
            sys.exit("Please provide a value for the PSR_ELK_URL  argument.")

        # Log input parameters
        logging.info(
            f"PSR_Envi: {psr_envi}, PSR_StartDate: {start_date}, PSR_EndDate: {end_date}, Tenant_Id: {tenant_id} , PSR_ELK_URL : {elk_url}")

        df = create_dataframe()

        response = get_data(psr_envi, api_key, tenant_id, start_date, end_date, elk_url)

        if response:
            df = process_data(response, df)
            save_to_csv(df, psr_envi, tenant_id)
            logging.info("Data processing completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
