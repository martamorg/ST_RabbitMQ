

#!/usr/bin/env python
import pika
import json

# defining what to do when a message is received
def callback(ch, method, properties, body):

    print(" [x] Received %r" % body)

    # decoding bytes into string and formatting into JSON
    body_str = body.decode('utf8').replace("'", '"')
    body_json = json.loads(body_str) #esto es un diccionario

    d_slow = {}
    d_fast = {}

    for key, item in body_json.items():
        if (key == "boat_speed" and item > 5):
            d_fast.update(body_json)
            d_fast_str = str(d_fast)
            ch.basic_publish(exchange='',
                      routing_key='marta_at_sea_stream',
                      body=str(d_fast_str))

            print(" [x] Sent: " + d_fast_str)
        elif (key == "boat_speed" and item <= 5):
            d_slow.update(body_json)
            d_slow_str = str(d_slow)
            ch.basic_publish(exchange='',
                      routing_key='marta_at_port_stream',
                      body=str(d_slow_str))
            print(" [x] Sent: " + d_slow_str)
    
    

    

    



def main():
    credentials = pika.PlainCredentials('zprojet', 'rabbit-22')
    parameters = pika.ConnectionParameters('rabbitmqserver', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # declaring input and output queues
    channel.queue_declare(queue='marta_boat_stream')
    channel.queue_declare(queue='marta_at_port_stream')
    channel.queue_declare(queue='marta_at_sea_stream')

    

    # auto_ack: as soon as collected, a message is considered as acked DO I WANNA CHANGE THIS ACK
    channel.basic_consume(queue='marta_boat_stream',
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
