#!/usr/bin/env python
import pika
import json

#definir un count para cada puerto 
c_BREST = []
c_VALENCIA =[]
c_PALERMO = []
c_BRIGHTON = []
c_AMSTERDAM = []

# defining what to do when a message is received
def callback(ch, method, properties, body):
    
    print(" [x] Received %r" % body)

    # decoding bytes into string and formatting into JSON
    body_str = body.decode('utf8').replace("'", '"')
    body_json = json.loads(body_str)

    for key, item in body_json.items():
        if (key == "boat_destination" and item == "BREST"):
            c_BREST.append(1)
        elif (key == "boat_destination" and item == "VALENCIA"):
            c_VALENCIA.append(1)
        elif (key == "boat_destination" and item == "PALERMO"):
            c_PALERMO.append(1)
        elif (key == "boat_destination" and item == "BRIGHTON"):
            c_BRIGHTON.append(1)
        elif (key == "boat_destination" and item == "AMSTERDAM"):
            c_AMSTERDAM.append(1)


    print(" [x] Port count: \n BREST: {} VALENCIA: {} PALERMO: {} BRIGHTON: {} AMSTERDAM: {}".format(len(c_BREST), len(c_VALENCIA), len(c_PALERMO), len(c_BRIGHTON), len(c_AMSTERDAM)))
    
            




   



def main():
    credentials = pika.PlainCredentials('zprojet', 'rabbit-22')
    parameters = pika.ConnectionParameters('rabbitmqserver', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # declaring input and output queues
    channel.queue_declare(queue='marta_at_port_stream')

    # auto_ack: as soon as collected, a message is considered as acked DO I WANNA CHANGE THIS
    channel.basic_consume(queue='marta_at_port_stream',
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
