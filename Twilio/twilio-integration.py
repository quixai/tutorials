from twilio.rest import *
from quixstreaming.models.parametersbufferconfiguration import ParametersBufferConfiguration
from quixstreaming import *
import signal
import threading
import time
import traceback

# quix settings
# todo get from environment variables
certificatePath = "../certificates/ca.cert"
username = "{placeholder:username}"
password = "{placeholder:password}"
broker = "kafka-k1.quix.ai:9093,kafka-k2.quix.ai:9093,kafka-k3.quix.ai:9093"

account_sid = "{placeholder:account_ssid}"
auth_token = "{placeholder:auth_token}"
messaging_service_sid = "{placeholder:service_ssid}"

message_limit_per_minute = 2  # Limit of how many messages per minute we allow to send.
messages_sent = []  # Epochs of messages sent.

# todo get from environment variables
commodity_id = "BTC-USD"  # eg BTC-USD?
threshold = 55000
phone_number = "{placeholder:your_phone_number}"

twilio_client = Client(account_sid, auth_token)

# Create a client factory. Factory helps you create StreamingClient (see below) a little bit easier
security = SecurityOptions(certificatePath, username, password)
client = StreamingClient(broker, security)

# To get more info about consumer group,
# see https://documentation.dev.quix.ai/quix-main/demo-quix-docs/concepts/kafka.html
consumer_group = "coinapi-alert-model-{0}-{1}".format(commodity_id, threshold)
input_topic = client.open_input_topic('{placeholder:input-topic}', consumer_group)

current_position = 0  # Current position of commodity. -1 for bellow threshold, 1 for above threshold.


def send_text_message(message):
    global messages_sent

    # when did we last send a message
    messages_sent = list(filter(lambda x: x > time.time() - 60, messages_sent))  # Filter epochs older than 60s.

    # make sure were not going over the rate limit of the free twilio account
    if len(messages_sent) < message_limit_per_minute:
        message = twilio_client.messages.create(
            messaging_service_sid=messaging_service_sid,
            body=message,
            to=phone_number
        )

        print("Message {0} sent to {1}".format(message, phone_number))
        messages_sent.append(time.time())
    else:
        print("Message {0} skipped due to message limit reached.".format(message))


# read from a quix stream
def read_stream(new_stream: StreamReader):
    print("New stream read:" + new_stream.stream_id)

    buffer_options = ParametersBufferConfiguration()

    # We are only interested in reacting to messages with values of commodity in question.
    buffer = new_stream.parameters.create_buffer(commodity_id, buffer_options)

    # create the callback to handle data being received by the stream
    def on_parameter_data_handler(data: ParameterData):
        try:
            global current_position

            df = data.to_panda_frame()

            # We iterate all rows and check if they cross threshold.
            for index, row in df.iterrows():
                timestamp = time.localtime(row["time"] / 1000000000)
                timestamp_str = time.strftime('%Y-%m-%d %H:%M:%S', timestamp)

                message = "At {0}, rate of {1} crossed the level of {2} with value {3}." \
                    .format(timestamp_str, commodity_id, threshold, row[commodity_id])

                if current_position == 0:  # We are starting, we going to set where we are against threshold.
                    current_position = 1 if row[commodity_id] > threshold else -1
                if current_position == 1:
                    if row[commodity_id] < threshold:  # We were above threshold, but now we are below.
                        send_text_message(message)
                        current_position = -1
                if current_position == -1:
                    if row[commodity_id] > threshold:  # We were bellow threshold, but now we are above.
                        send_text_message(message)
                        current_position = 1
        except Exception:
            print(traceback.format_exc())

    buffer.on_read += on_parameter_data_handler


# Hook up events before initiating read to avoid losing out on any data
input_topic.on_stream_received += read_stream
input_topic.start_reading()  # initiate read

# Hook up to termination signal (for docker image) and CTRL-C
print("Listening to streams. Press CTRL-C to exit.")

event = threading.Event()


def signal_handler(sig, frame):
    print('Exiting...')
    event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
event.wait()
