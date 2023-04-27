from datetime import timedelta
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

env = StreamExecutionEnvironment.get_execution_environment()

# create a FlinkKafkaConsumer instance
consumer = FlinkKafkaConsumer(
    'near_earth_objects', 
    SimpleStringSchema(), 
    properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'my-group'}
)

# create a data stream from the Kafka topic
data_stream = env.add_source(consumer)

# apply various operations on the data stream
# e.g., filter out asteroids with a diameter smaller than 1000 meters
filtered_stream = data_stream \
    .filter(lambda x: float(x.split(',')[2]) >= 1000)

# perform window aggregation, e.g., compute the average diameter of asteroids over a sliding window of 5 minutes
windowed_stream = filtered_stream \
    .map(lambda x: (1, float(x.split(',')[2]))) \
    .key_by(lambda x: x[0]) \
    .time_window(window_size=timedelta(minutes=5)) \
    .reduce(lambda x, y: (x[0], x[1] + y[1], x[2] + y[2])) \
    .map(lambda x: (x[0], x[1] / x[2]))

# print the result to the console
windowed_stream.print()

# execute the Flink job
env.execute()

