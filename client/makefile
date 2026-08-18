CFLAGS:=-c -Wall -Weffc++ -g -std=c++11 -Iinclude
LDFLAGS:=-lboost_system -lpthread

all: StompWCIClient

StompWCIClient: bin/ConnectionHandler.o bin/StompClient.o bin/StompProtocol.o bin/event.o
	g++ -o bin/StompWCIClient bin/ConnectionHandler.o bin/StompClient.o bin/StompProtocol.o bin/event.o $(LDFLAGS)

bin/ConnectionHandler.o: src/ConnectionHandler.cpp
	g++ $(CFLAGS) -o $@ $<

bin/StompClient.o: src/StompClient.cpp
	g++ $(CFLAGS) -o $@ $<

bin/StompProtocol.o: src/StompProtocol.cpp
	g++ $(CFLAGS) -o $@ $<

bin/event.o: src/event.cpp
	g++ $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f bin/*