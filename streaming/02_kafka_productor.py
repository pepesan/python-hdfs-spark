# Requiere: docker/01_launch.sh (servicio kafka).
#
# Pequeño productor de mensajes Kafka — lo usan tanto el test automatizado
# como 03_structured_streaming_kafka.py para tener algo que consumir
# (Kafka, a diferencia de la fuente "rate", no genera datos él solo: hace
# falta un productor que escriba en el topic).
#
# No es un ejemplo de Spark (no usa pyspark en absoluto): es un cliente
# Kafka normal y corriente, con el paquete kafka-python.
import sys
import time

from kafka import KafkaProducer

if __name__ == '__main__':
    productor = KafkaProducer(bootstrap_servers='localhost:9092')

    frases = [
        'spark rdd example',
        'spark streaming example',
        'kafka spark ejemplo',
    ]
    if len(sys.argv) > 1:
        frases = sys.argv[1:]

    for frase in frases:
        # Kafka trabaja con bytes, no con texto directamente — hay que
        # codificar cada mensaje explícitamente
        productor.send('frases', frase.encode('utf-8'))
        print(f'Enviado: {frase}')
        time.sleep(0.2)

    # flush() espera a que Kafka confirme la recepción de todos los
    # mensajes antes de continuar — sin esto, el proceso podría cerrarse
    # antes de que el envío haya terminado de verdad
    productor.flush()
    productor.close()
