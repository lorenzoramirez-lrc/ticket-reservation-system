CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra

all:
	mkdir -p build
	$(CXX) $(CXXFLAGS) src/servicios-almacenamiento/servicio-persistencia.cpp -o build/persistencia -lzmq -lpqxx 
	$(CXX) $(CXXFLAGS) src/servicios-negocio/gestor-transacciones.cpp -o build/gestor -lzmq
	$(CXX) $(CXXFLAGS) src/servicios-negocio/servicio-rmc.cpp -o build/rmc -lzmq

clean: 
	rm -rf build


