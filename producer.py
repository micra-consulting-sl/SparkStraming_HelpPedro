from time import sleep
from json import dumps
from kafka import KafkaProducer
from faker import Faker                                         #Libreria para crear random dataset, importar pandas y guardarlo en un dataframe para poder realizar operaciones
import keyboard                                                 #Se utiliza para obtener el control toral del teclado, Para asignar a una tecla una función para que sea interactivo
import time
import random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

faker = Faker('es_ES')                                          #Solo esta generando en Español
USERS_TOTAL=100                                                 #Num total de usuarios es de 100
users={}                                                        #Se almacena todo en esta variable
lat_min=39.4                                                    #Solo nos dan latitud min; Pedir mas
lat_max=39.5                                                    #Solo nos dan latitud max; Pedir mas
lon_min=-0.3                                                    #Solo nos dan longitud min; Pedir mas
lon_max=-0.4                                                    #Solo nos dan longitud max; Pedir mas
vehicles=["Bike","Train","Car", "Walking"]                      #Nos dan como se mueven los usuarios, solo nos interesa Bike y Walking; 
#Pedir mas VARIABLES

def initiate_data():
    global users
    for i in range(0,USERS_TOTAL):                              #Bucle para generar los 100 usuarios
        user={}                                                 #Almacen de los valores
        user["id"]=faker.ssn()                                  #ID de la persona (num ss)
        user["name"]=faker.first_name()                         #Nombre
        user["last_name"]=faker.last_name()                     #Apellido
        # user["friends"]=[]                                    #NO CORRESPONDE 
        user["position"]={"lat":random.uniform(39.4, 39.5),"lon":random.uniform(-0.3, -0.4)}    #Posicion, genera una longitud y una latitud entre esos rangod de forma aleatoria
        user["transport"]=random.choice(vehicles)               #Seleccion del medio de transporte de manera aleatoria (Puede ser ambos?)
        users[user["id"]]=user                                 #??
        

    #La siguiente funcion no corresponde a nuestro caso
    # num=0
    # for element in users.items():
    #     print(f"Generating friends of {num} of {len(users)}")
    #     for i in range(0,random.randint(1,10)):
    #         friend=random.choice(list(users.values()))
    #         if friend["id"]!=element[0]:
    #             users[element[0]]["friends"].append(friend["id"])
    #         else:
    #             print("No friend of yourself") 
    #     num=num+1
    

    print("DATA GENERATED")        


def generate_step():
    global users
    if len(users)>0:                                            #Si la longitud de nuestro usuario es 0, no hay nada escrito sobre el. Evita errores.
        print("STEP")
        for element in users.items():                           # .items metodo que se usa para devolver una lista con todas las claves y valores del diccionario
            lat=users[element[0]]["position"]["lat"]
            lon=users[element[0]]["position"]["lon"]
            users[element[0]]["position"]["lon"]=lon+random.uniform(0.001, 0.005) #Suma de pasos por cada vez que se repita el buble
            users[element[0]]["position"]["lat"]=lat+random.uniform(0.001, 0.005) #Suma de pasos por cada vez que se repita el buble
            if lat>lat_max or lat<lat_min:
                users[element[0]]["position"]["lat"]=random.uniform(39.4, 39.5)
                users[element[0]]["transport"]=random.choice(vehicles)
            if lon>lon_max or lon<lon_min:
                users[element[0]]["position"]["lon"]=random.uniform(-0.3, -0.4)
    else:
        initiate_data()
    return users

def remove_keys_with_empty_values(users):
  if hasattr(users, 'items'):
    return {key: remove_keys_with_empty_values(value) for key, value in users.items() if value==0 or value}
  elif isinstance(users, list):
    return [remove_keys_with_empty_values(value) for value in users if value==0 or value]
  else:
    return users


# NO TOCAR

while True:
    try:  
        if keyboard.is_pressed('q'):  # if key 'q' is pressed 
            print('You Exited the data generator')
            break  
        else:
            users_generated=generate_step()
            #EnterCode
            key = "position"

            # using keys() and values() to extract values
            res = [sub[key] for sub in users.values() if key in sub.keys()]

            print(res)
            producer.send('topic1', value=res)
            print("code")
            # End Place for code
            time.sleep(2)
    except Exception as err:
        print(f"Unexpected {err}, {type(err)}")
        break  