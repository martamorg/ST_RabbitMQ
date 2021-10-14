#!/usr/bin/env python
import pika
import json

SPEEDS = []

# defining what to do when a message is received
def callback(ch, method, properties, body):

    print(" [x] Received %r" % body)

    # decoding bytes into string and formatting into JSON
    body_str = body.decode('utf8').replace("'", '"')
    body_json = json.loads(body_str)

    AVG_SPEED = 0
    THESPEEDS = 0

    for key, item in body_json.items():
        if (key == "boat_speed"):
            SPEEDS.append(int(item))

    for x in SPEEDS:
        THESPEEDS = THESPEEDS + x
        AVG_SPEED = (THESPEEDS)/len(SPEEDS)
    
    print(" [x] Average speed: %r" % AVG_SPEED)




def main():
    credentials = pika.PlainCredentials('zprojet', 'rabbit-22')
    parameters = pika.ConnectionParameters('rabbitmqserver', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # declaring input and output queues
    channel.queue_declare(queue='marta_at_sea_stream')

    # auto_ack: as soon as collected, a message is considered as acked DO I WANNA CHANGE THIS
    channel.basic_consume(queue='marta_at_sea_stream',
                      auto_ack=True,
                      on_message_callback=callback)

    # wait for messages
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()



        


if __name__ == '__main__':
    try:
                
        main()

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
