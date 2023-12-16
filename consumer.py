import logging
from confluent_kafka import Consumer, KafkaError
import json
import xlwt
from xlwt import Workbook
from xlutils.copy import copy
import xlrd

# Configure logging to write messages to a file
logging.basicConfig(filename='kafka_consumer.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

consumer = Consumer({
   'bootstrap.servers': '192.168.53.196:9092',  # Replace with your Kafka broker address
   'group.id': 'my-consumer-group',
   'auto.offset.reset': 'earliest'  # You can adjust this based on your requirements
})

topic = 'chats'  # Replace with the Kafka topic you want to consume
wbr = xlrd.open_workbook('Row.xls', on_demand=True)
wb1 = copy(xlrd.open_workbook('Chats.xls', on_demand=True))
wbs = wbr.sheet_by_index(0)
sheet1 = wb1.get_sheet(0)
row = wbs.cell_value(0,0)
row = int(row)
wbr.release_resources()
del wbr
def kafka_consumer(row):
    consumer.subscribe([topic])
    connected = True
    while connected:
        try:
            msg = consumer.poll(1.0)

            if msg is None:
               continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Kafka error: {msg.error()}")
                    break

           # Process the Kafka message (you can customize this part)
            message_value = msg.value().decode('utf-8')
            print(f"[RECEIVED] -----> {message_value}")
            logging.info(f"Received message: {message_value}")
            message_value = json.loads(message_value)
            col = 0
            for _ , val in message_value.items():
                sheet1.write(row, col, val)
                col += 1
            wb1.save('Chats.xls')
            row += 1

        except KeyboardInterrupt:
            connected = False

    return row


if __name__ == '__main__':
    row = kafka_consumer(row)
    wb1 = Workbook()
    sheetr = wb1.add_sheet('Sheet1')
    sheetr.write(0, 0, row)
    wb1.save('Row.xls')
