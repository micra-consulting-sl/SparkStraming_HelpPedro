**Para levantar el contenedor de Postgres introducir en la terminal:**

docker container run -d --name=postgres -p 5432:5432 -e POSTGRES_PASSWORD=secret -e PGDATA=/pgdata -v /pgdata:/pgdata postgres:11.4

user: postgres

password: secret



Ya tendriamos levantado el server de postgres. Para visualizar la base de datos descargar DBeaver he introducir usuario.



**Codigo python:**

El codigo "pruebaconexion.py" que hay es un test de conexión de un script de python a la base de datos.

Librerias necesarias: 

- pip install psycodg2

- pip install pygresql (por si acaso la utilizamos más adelante

Este código es el que se tiene que implementar a nuestro consumer.py  .



