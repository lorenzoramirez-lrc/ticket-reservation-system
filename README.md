# Sistema Distribuido para Reserva de Eventos 

Sistema distribuido para manejo de reservas, cancelaciones, modificaciones y consultas de disponibilidad de eventos.

El proyecto incorpora comunicacion distribuida, almacenamiento persistente, y procesamiento concurrente.

## Caracteristicas
El sistema soporta las siguientes operaciones:

- Reservar un numero de asientos para un evento.
- Cancelar reservaciones existentes.
- Modificar reservas.
- Obtener informacion de eventos o reservas.

## Tecnologias

- C++ — servicios distribuidos.
- Python — clientes.
- ØMQ — comunicacion entre procesos.
- PostgreSQL — servicio de persistencia.
- Docker Compose — entorno de la base de datos.
- Locust — generacion de carga.


## Estructura
```
.
├── database
│   ├── docker-compose.yaml
│   ├── fill_database.py
│   └── init.sql
├── docs
│   └── diagrams
│       ├── cancelreservation.png
│       ├── clases.png
│       ├── componentes.png
│       ├── despliegue.png
│       ├── makereservation.png
│       ├── modifyOcurrence.png
│       └── modifyquantity.png
├── Makefile
├── README.md
└── src
    ├── clients
    │   ├── client.py
    │   ├── generate_data.py
    │   └── reserve_scenarios.json
    ├── request-structs
    │   └── structs.cpp
    ├── servicios-almacenamiento
    │   └── servicio-persistencia.cpp
    └── servicios-negocio
        ├── gestor-transacciones.cpp
        └── servicio-rmc.cpp
```

## Requerimientos

Se requiere la instalacion de las siguientes dependencias.

**Gestor de Paquetes del Sistema Operativo:**
- libzmq3-dev 
- libpqxx-dev

**Gestor de Paquetes Python:**
- Locust

Asimismo se requiere de una version compatible de docker para la base de datos.

## Compilación

1. Clonar repositorio ejecutando ```git clone https://github.com/lorenzoramirez-lrc/ticket-reservation-system.git```
2. Ejecutar ```make clean``` y ```make all``` para compilar ejecutables de C++ estos se encontraran en el directorio build/

## Ejecución

El sistema se ejecuta en tres maquinas diferentes en una misma red local. Para conocer distribucion
de los servicios referirse a [`docs/diagrams/despliegue.png`](docs/diagrams/despliegue.png).

1. Levantar base de datos ejecutando desde database/ ```docker compose up -d```. En caso de que se quiera iniciar el servicio desde cero
se debe ejecutar ```docker compose down -v``` y nuevamente ```docker compose up -d```.
2. La base de datos contiene dos eventos de prueba para un pequeño escenario de simulacion. Sin embargo se puede generar una mayor carga de eventos
utilizando ```database/fill_database.py```.
3. Ejecutar el servicio de persistencia ```./persistencia <ip-persistencia>```.
4. Ejecutar el servicio RMC ```./rmc <ip-rmc> <ip-persistencia>```.
5. Ejecutar el servicio de gestion de transacciones ```./gestor <ip-gestor> <ip-rmc>```.
6. Ejecutar el script de generacion de datos para los clientes y eventos ```python generate_data.py```
7. En caso de realizar el pequeño escenario de simulación ejecutar el siguiente comando ```MANAGER_ADDR=<ip-gestor> SCENARIO=x locust -f client.py headless -u 1 -r 1 -t 5s ReserveUser```
   Donde x es cualquier nombre de un escenario de simulacion, u el numero de usuarios simulados, r el numero de usuarios que se agregan por segundo y t el tiempo de ejecucion de la prueba.
8. En cualquier otro caso se llena la base de datos con los datos generados en generate_data.py ```python fill_database.py``` y se utiliza
   el siguiente comando ```MANAGER_ADDR=<ip-gestor> QUERY_USERS=1 MIXED_USERS=1 RESERVE_USERS=1\
locust -f client.py --headless -u 3 -r 3 -t 3m QueryUser MixedUser ReserveUser```.
El numero de usuarios en -u debe coincidir con el total de query, mixed y reserve users.
Adicionalmente, se pueden definir los parametros correspondientes a la cantidad de solicitudes por minuto y el porcentaje de consultas que realiza QueryUser,
agregando al inicio del comando las variables ```REQS_PER_MIN=x MIXED_QUERY_PCT=y```.


## Autores
Lorenzo Ramírez Calderón, Richard Manuel Castillo Pesca, Nicolas Sanchez Luengas
