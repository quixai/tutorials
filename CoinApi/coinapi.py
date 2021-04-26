import traceback
import time
import requests
from dateutil.parser import parse
import os
from quixstreaming import *


def connect_to_quix():
    certificatePath = "../certificates/ca.cert"
    username = "{placeholder:username}"
    password = "{placeholder:password}"
    broker = "kafka-k1.quix.ai:9093,kafka-k2.quix.ai:9093,kafka-k3.quix.ai:9093"

    security = SecurityOptions(certificatePath, username, password)
    client = StreamingClient(broker, security)

    # the topic where we will send this data
    output_topic = client.open_output_topic('{placeholder:output-topic}')

    # create a stream
    return output_topic.create_stream("coin-api-data")


def stream_coin_rates_to_quix(quix_stream):

    # todo get from environment variables
    api_key = os.environ.get("coinapi_key")

    from_currency = os.environ.get("from_currency")  # todo get from environment variables
    to_currency = os.environ.get("to_currencys")  # todo get from environment variables

    url = 'https://rest.coinapi.io/v1/exchangerate/{0}?filter_asset_id={1}'.format(from_currency, to_currency)
    headers = {'X-CoinAPI-Key': api_key}

    while True:

        try:
            response = requests.get(url, headers=headers)

            data = response.json()

            if 'error' in data:
                raise Exception("Error", data["error"])

            rows = data['rates']

            for row in rows:
                # For every currency we send value.
                quix_stream.parameters.buffer.add_timestamp(parse(row['time'])) \
                    .add_value("{0}-{1}".format(from_currency, row['asset_id_quote']), row['rate']) \
                    .write()

                print("{0}-{1}: {2}".format(from_currency, row['asset_id_quote'], row['rate']))

        except Exception:
            print(traceback.format_exc())

        time.sleep(900)  # We sleep for 15 minute not to reach free account limit.


stream = connect_to_quix()
stream_coin_rates_to_quix(stream)

