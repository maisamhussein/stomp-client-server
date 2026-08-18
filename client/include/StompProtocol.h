#pragma once
#include <string>
#include <vector>
#include <map>
#include <atomic>
#include <mutex>
#include <memory>

#include "ConnectionHandler.h"
#include "event.h"  

class StompProtocol {
public:
    StompProtocol();
    ~StompProtocol();

    void processCommands(std::string input);
    void socketReader();
    void close();

private:
    static std::vector<std::string> splitInput(const std::string& input, char sep);
    static std::string trim(const std::string& s);

    static bool startsWith(const std::string& s, const std::string& pref);
    static std::string headerValue(const std::string& frame, const std::string& header);

    void storeEventLocal(const std::string& game, const std::string& user, const Event& ev);

    void writeSummaryFile(const std::string& game, const std::string& user, const std::string& filePath);

    std::shared_ptr<ConnectionHandler> handler;
    std::string username;
    std::atomic<bool> connected;

    std::atomic<int> idCounter; 
    std::map<std::string,int> channelToId; 
    std::map<int,std::string> idToChannel; 

    std::atomic<int> receiptCounter;
    std::map<int, std::string> receiptToAction; 
    
    std::map<std::string, std::map<std::string, std::vector<Event>>> eventsDB;

    std::map<std::string, std::pair<std::string,std::string>> gameTeams;

    std::mutex mtx; 
};
