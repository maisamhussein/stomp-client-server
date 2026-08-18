#pragma once

#include <map>
#include <vector>
#include <string>
#include "event.h"

struct GameEvent {
    int time;
    std::string name;
    std::string description;
};

struct GameData {
    std::string teamA;
    std::string teamB;

    std::map<std::string, std::string> generalStats;
    std::map<std::string, std::string> teamAStats;
    std::map<std::string, std::string> teamBStats;

    std::vector<GameEvent> events;
};