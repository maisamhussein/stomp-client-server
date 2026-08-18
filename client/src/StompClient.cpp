#include <thread>
#include <iostream>
#include "../include/StompProtocol.h"

using namespace std;

// Thread שקורא הודעות מהשרת 
static void socketReaderLoop(StompProtocol &protocol) {
    while (true) {
        protocol.socketReader();
    }
}

// Thread שקורא קלט מהמשתמש
static void keyboardLoop(StompProtocol &protocol) {
    string input;
    while (getline(cin, input)) {
        protocol.processCommands(input);
    }
}

int main(int argc, char *argv[]) {
    StompProtocol protocol;
    // Thread for reading messages from the server
    thread tSocket(socketReaderLoop, ref(protocol));
    // Thread for reading commands from the keyboard
    thread tKeyboard(keyboardLoop, ref(protocol));

    tKeyboard.join();  
    protocol.close();   
    tSocket.detach();   

    return 0;
}
