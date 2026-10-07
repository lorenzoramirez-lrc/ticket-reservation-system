all:
	mkdir build
	g++ src/servicios-almacenamiento/servicio-persistencia.cpp -o build/persistencia -lzmq
	g++ src/servicios-negocio/gestor-transacciones.cpp -o build/gestor -lzmq
	g++ src/servicios-negocio/servicio-rmc.cpp -o build/rmc -lzmq

clear:
	rm -rf build


