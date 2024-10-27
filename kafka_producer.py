#Importamos las dependencias necesarias desde Kafka a este producer
import time
import json
import random
from kafka import KafkaProducer
#Esta función genera los datos del sensor
def generate_sensor_data():
 return {
 "sensor_id": random.randint(1, 10),
 "temperature": round(random.uniform(20, 30), 2),
 "humidity": round(random.uniform(30, 70), 2),
 "timestamp": int(time.time())
 }
 #Se crea el productor de Kafka y se conecta al servidor en localhost:9092
 #Convierte los datos a JSON y luego a bytes para enviarlos a Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
 value_serializer=lambda x: json.dumps(x).encode('utf-8'))
 #Se crea un bucle infinito que envía constantemente datos de sensores continuamente
while True:
 #Genera los datos del sensor
 sensor_data = generate_sensor_data()
 #Envia los datos al Topic "sensor_data" de Kafka
 producer.send('sensor_data', value=sensor_data)
 #Imprime los datos enviados en la consola
 print(f"Sent: {sensor_data}")
 #Espera 1 segundo antes para enviar el siguiente dato
 time.sleep(1)
