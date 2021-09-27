import psycopg2 as psycopg

import json

from datetime import datetime, timedelta
import pytz

import pandas

import getpass

DB_NAME = 'observer_20210921_143000'
DB_USER = getpass.getuser()
#DB_USER = 'pgsql' # put yourusername

RESULT_CSV_FILENAME = './data/elec-duma-1-mandate-moscow-votes-per-second.csv'
BALLOTS_CONF_FILENAME = './data/ballots-conf-federal-duma-1-mandate-moscow.json'

LOCAL_TZ  = pytz.timezone('Europe/Moscow')

# timestamps are TZ naive by default
# once initialized, TZ is explicitely set to the LOCAL_TZ
elections_start_time = datetime(2021, 9, 17, 8, 0, 0, 0)
elections_start_time = LOCAL_TZ.localize( elections_start_time )

elections_end_time   = datetime(2021, 9, 19, 20, 0, 0, 0)
elections_end_time   = LOCAL_TZ.localize( elections_end_time )

# Counting the number of seconds between start and end timestamps
elec_duration = elections_end_time - elections_start_time
elec_duration_seconds = elec_duration.days*24*3600 + elec_duration.seconds

# Create resulting pandas dataframe

result_df = pandas.DataFrame()

# Reading data from pgsql

with psycopg.connect("dbname=" + DB_NAME + " user=" + DB_USER) as psql_conn:
    with psql_conn.cursor() as cur:

        # Getting ballot config (list of districts and candidates for each)

        SQL_QUERY = "SELECT payload FROM transactions WHERE method_id=0;"
        cur.execute(SQL_QUERY)

        ballots = cur.fetchone()

        # Data from psql is a tuple, even if there is only one column
        # So the 0th element is of interest
        ballots_conf = ballots[0]["ballots_config"]

        with open(BALLOTS_CONF_FILENAME, 'w') as json_file:
            json.dump(ballots_conf, json_file)

        # Iterate through districts
        for district in ballots_conf:

            print ( "# District #", district["district_id"] )

            candidates = district["options"]

            # Iterate through candidates of the district
            for candidate_id in candidates:

                # Intitalize an array of elec_duration_seconds elements = 0
                # Each element will correspond to a second of elections duration

                votes_per_second = [0] * elec_duration_seconds

                tt = []
                for idx in range(0, elec_duration_seconds):
                    tt.append( (elections_start_time + timedelta(seconds=idx)).astimezone(LOCAL_TZ).strftime('%Y-%m-%d-%H-%M-%S') )

                result_df['Time'] = tt

                # Getting candidate's results
                SQL_QUERY = """
                            SELECT datetime
                            FROM public.decrypted_ballots as ballot
                            JOIN public.transactions as trans_store
                            ON (ballot.store_tx_hash = trans_store.hash)
                            WHERE ballot.decrypted_choice[1] =""" + candidate_id + """
                            ORDER BY datetime;
                            """

                cur.execute(SQL_QUERY)

                votes_time = cur.fetchall()

                # SQL returns a list of tuples. So we need a list of all first (0-index) elements of all tuples
                votes_time = [item[0] for item in votes_time]

                print ( "#", candidate_id, candidates[candidate_id], len(votes_time) )

                # Iterate through all votes for a given candidate and count number of votes for each 1-second interval of elections
                for vote_t in votes_time:
                    # Counting the number of seconds between start and end timestamps
                    vote_time_shift = vote_t - elections_start_time
                    vote_sec_idx    = vote_time_shift.days*24*3600 + vote_time_shift.seconds # The number of seconds since the elections start

                    # Increment the number of votes for the given second
                    votes_per_second[vote_sec_idx] = votes_per_second[vote_sec_idx] + 1

                culumn_header = candidates[candidate_id] + '. Округ ' + str( district["district_id"] )
                result_df[culumn_header] = votes_per_second

result_df.to_csv(RESULT_CSV_FILENAME, index=False)
