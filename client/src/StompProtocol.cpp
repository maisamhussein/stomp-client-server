#include "StompProtocol.h"
#include <sstream>
#include <iostream>
#include <fstream>
#include <algorithm>

using std::cerr;
using std::cout;
using std::endl;
using std::map;
using std::string;
using std::vector;

static bool parseHostPort(const std::string &hp, std::string &host, short &port)
{
    auto pos = hp.find(':');
    if (pos == std::string::npos)
        return false;
    host = hp.substr(0, pos);
    std::string p = hp.substr(pos + 1);
    try
    {
        int prt = std::stoi(p);
        if (prt <= 0 || prt > 65535)
            return false;
        port = (short)prt;
        return true;
    }
    catch (...)
    {
        return false;
    }
}

StompProtocol::StompProtocol()
    : handler(nullptr),
      username(""),
      connected(false),
      idCounter(1),
      channelToId(),
      idToChannel(),
      receiptCounter(1),
      receiptToAction(),
      eventsDB(),
      gameTeams(),
      mtx()
{
}



StompProtocol::~StompProtocol()
{
    close();
}

string StompProtocol::trim(const string &s)
{
    size_t b = s.find_first_not_of(" \t\r\n");
    if (b == string::npos)
        return "";
    size_t e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

vector<string> StompProtocol::splitInput(const string &input, char sep)
{
    vector<string> out;
    std::stringstream ss(input);
    string part;
    while (std::getline(ss, part, sep))
        out.push_back(part);
    return out;
}

bool StompProtocol::startsWith(const string &s, const string &pref)
{
    return s.size() >= pref.size() && s.compare(0, pref.size(), pref) == 0;
}

string StompProtocol::headerValue(const string &frame, const string &header)
{
    auto lines = splitInput(frame, '\n');
    for (const string &raw : lines)
    {
        string line = trim(raw);
        if (line.empty())
            break;
        auto pos = line.find(':');
        if (pos == string::npos)
            continue;
        string key = trim(line.substr(0, pos));
        string val = trim(line.substr(pos + 1));
        if (key == header)
            return val;
    }
    return "";
}

// Stores received event in client memory
// Used later by the summary command
void StompProtocol::storeEventLocal(const string &game, const string &user, const Event &ev)
{
    eventsDB[game][user].push_back(ev);
    auto &v = eventsDB[game][user];
    std::sort(v.begin(), v.end(), [](const Event &a, const Event &b)
              { return a.get_time() < b.get_time(); });
}

void StompProtocol::writeSummaryFile(const string &game, const string &user, const string &filePath)
{
    std::ofstream out(filePath);
    if (!out.is_open())
    {
        cout << "Failed to open the file" << endl;
        return;
    }

    vector<Event> events;
    std::pair<string, string> teams{"", ""};
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (eventsDB.count(game) && eventsDB[game].count(user))
        {
            events = eventsDB[game][user];
        }
        if (gameTeams.count(game))
            teams = gameTeams[game];
    }

    if (events.empty())
    {
        cout << "No data for this game/user" << endl;
        out.close();
        return;
    }

    if (teams.first.empty() || teams.second.empty())
    {
        teams.first = events[0].get_team_a_name();
        teams.second = events[0].get_team_b_name();
    }

    map<string, string> generalStats, teamAStats, teamBStats;

    for (const auto &ev : events)
    {
        for (const auto &p : ev.get_game_updates())
            generalStats[p.first] = p.second;
        for (const auto &p : ev.get_team_a_updates())
            teamAStats[p.first] = p.second;
        for (const auto &p : ev.get_team_b_updates())
            teamBStats[p.first] = p.second;
    }

    out << teams.first << " vs " << teams.second << "\n";
    out << "Game stats:\n";
    out << "General stats:\n";
    for (const auto &p : generalStats)
        out << p.first << ": " << p.second << "\n";

    out << teams.first << " stats:\n";
    for (const auto &p : teamAStats)
        out << p.first << ": " << p.second << "\n";

    out << teams.second << " stats:\n";
    for (const auto &p : teamBStats)
        out << p.first << ": " << p.second << "\n";

    out << "Game event reports:\n";
    for (const auto &ev : events)
    {
        out << ev.get_time() << " - " << ev.get_name() << ":\n";
        out << ev.get_description() << "\n";
    }

    out.close();
}

// Handles user commands from the keyboard
void StompProtocol::processCommands(string input)
{
    input = trim(input);
    if (input.empty())
        return;

    auto parts = splitInput(input, ' ');
    string cmd = parts[0];

    if (cmd != "login" && !connected.load())
    {
        cout << "please login first" << endl;
    }

    // login: Opens socket connection and sends CONNECT frame to server
    if (cmd == "login")
    {
        if (parts.size() != 4)
        {
            cout << "Usage: login {host:port} {username} {password}" << endl;
            return;
        }
        if (connected.load())
        {
            cout << "The client is already logged in, log out before trying again" << endl;
            return;
        }

        std::string host;
        short port;
        if (!parseHostPort(parts[1], host, port))
        {
            cout << "Invalid host:port" << endl;
            return;
        }

        std::lock_guard<std::mutex> lock(mtx);
        handler = std::make_shared<ConnectionHandler>(host, port);
        if (!handler->connect())
        {
            handler.reset();
            cout << "Failed to login, try again" << endl;
            return;
        }

        username = parts[2];
        connected.store(true);

        std::ostringstream frame;
        frame << "CONNECT\n"
              << "accept-version:1.2\n"
              << "host:stomp.cs.bgu.ac.il\n"
              << "login:" << parts[2] << "\n"
              << "passcode:" << parts[3] << "\n\n";

        handler->sendFrameAscii(frame.str(), '\0');
        return;
    }

    if (cmd == "summary")
    {
        if (parts.size() != 4)
        {
            cout << "summary command needs 3 args: {game_name} {user} {file}" << endl;
            return;
        }
        writeSummaryFile(parts[1], parts[2], parts[3]);
        return;
    }

    std::shared_ptr<ConnectionHandler> h;
    {
        std::lock_guard<std::mutex> lock(mtx);
        h = handler;
    }
    if (!h)
        return;

    // join: Subscribes client to a topic using SUBSCRIBE frame
    if (cmd == "join")
    {
        if (parts.size() != 2)
        {
            cout << "join command needs 1 arg: {topic}" << endl;
            return;
        }

        std::lock_guard<std::mutex> lock(mtx);

        const string &topic = parts[1];

        if (channelToId.count(topic))
        {
            cout << "Already subscribed to " << topic << endl;
            return;
        }

        int subId = idCounter.fetch_add(1);
        int rid = receiptCounter.fetch_add(1);

        channelToId[topic] = subId;
        idToChannel[subId] = topic;
        receiptToAction[rid] = "join " + topic;

        std::ostringstream frame;
        frame << "SUBSCRIBE\n"
              << "destination:" << topic << "\n"
              << "id:" << subId << "\n"
              << "receipt:" << rid << "\n\n";

        h->sendFrameAscii(frame.str(), '\0');
        return;
    }

    if (cmd == "exit")
    {
        if (parts.size() != 2)
        {
            cout << "exit command needs 1 arg: {topic}" << endl;
            return;
        }

        std::lock_guard<std::mutex> lock(mtx);
        const string &topic = parts[1];
        if (!channelToId.count(topic))
        {
            cout << "Not subscribed to " << topic << endl;
            return;
        }

        int subId = channelToId[topic];
        int rid = receiptCounter.fetch_add(1);
        receiptToAction[rid] = "exit " + topic;

        std::ostringstream frame;
        frame << "UNSUBSCRIBE\n"
              << "id:" << subId << "\n"
              << "receipt:" << rid << "\n\n";

        h->sendFrameAscii(frame.str(), '\0');
        return;
    }

    // report: Sends events to the channel the client is currently subscribed to
    if (cmd == "report")
    {
        if (parts.size() != 2)
        {
            cout << "report command needs 1 arg: {file}" << endl;
            return;
        }

        string topic;
        {
            std::lock_guard<std::mutex> lock(mtx);
            if (channelToId.empty())
            {
                cout << "Not subscribed to any channel" << endl;
                return;
            }
            topic = channelToId.begin()->first;
        }

        names_and_events nne = parseEventsFile(parts[1]);

        {
            std::lock_guard<std::mutex> lock(mtx);
            string game = nne.team_a_name + "_" + nne.team_b_name;
            gameTeams[game] = {nne.team_a_name, nne.team_b_name};
        }

        for (const auto &ev : nne.events)
        {

            std::ostringstream body;
            body << "user:" << username << "\n"
                 << "team a:" << nne.team_a_name << "\n"
                 << "team b:" << nne.team_b_name << "\n"
                 << "event name:" << ev.get_name() << "\n"
                 << "time:" << ev.get_time() << "\n"
                 << "general game updates:\n";

            for (const auto &p : ev.get_game_updates())
                body << p.first << ":" << p.second << "\n";

            body << "team a updates:\n";
            for (const auto &p : ev.get_team_a_updates())
                body << p.first << ":" << p.second << "\n";

            body << "team b updates:\n";
            for (const auto &p : ev.get_team_b_updates())
                body << p.first << ":" << p.second << "\n";

            body << "description:\n"
                 << ev.get_description() << "\n";

            std::ostringstream frame;
            frame << "SEND\n"
                  << "destination:" << topic << "\n\n"
                  << body.str();

            h->sendFrameAscii(frame.str(), '\0');
        }
        return;
    }

    if (cmd == "logout")
    {
        if (parts.size() != 1)
        {
            cout << "logout command needs 0 args" << endl;
            return;
        }

        std::lock_guard<std::mutex> lock(mtx);
        int rid = receiptCounter.fetch_add(1);
        receiptToAction[rid] = "logout";

        std::ostringstream frame;
        frame << "DISCONNECT\n"
              << "receipt:" << rid << "\n\n";

        h->sendFrameAscii(frame.str(), '\0');
        return;
    }

    cout << "Illegal command, please try a different one" << endl;
}

void StompProtocol::socketReader()
{
    std::shared_ptr<ConnectionHandler> h;
    {
        std::lock_guard<std::mutex> lock(mtx);
        h = handler;
    }
    if (!h || !connected.load())
        return;

    string frame;
    if (!h->getFrameAscii(frame, '\0'))
        return;

    auto lines = splitInput(frame, '\n');
    if (lines.empty())
        return;

    string cmd = trim(lines[0]);

    if (cmd == "CONNECTED")
    {
        cout << "Login successful" << endl;
        return;
    }

    if (cmd == "RECEIPT")
    {
        int rid = -1;
        for (const auto &l : lines)
        {
            string line = trim(l);
            if (startsWith(line, "receipt-id:"))
            {
                rid = std::stoi(trim(line.substr(string("receipt-id:").size())));
                break;
            }
        }

        string action;
        {
            std::lock_guard<std::mutex> lock(mtx);
            if (receiptToAction.count(rid))
            {
                action = receiptToAction[rid];
                receiptToAction.erase(rid);
            }
        }

        if (startsWith(action, "join "))
        {
            cout << "Joined channel " << action.substr(5) << endl;
        }
        else if (startsWith(action, "exit "))
        {
            cout << "Exited channel " << action.substr(5) << endl;

            std::lock_guard<std::mutex> lock(mtx);
            string topic = action.substr(5);
            if (channelToId.count(topic))
            {
                int sid = channelToId[topic];
                channelToId.erase(topic);
                idToChannel.erase(sid);
            }
        }
        else if (action == "logout")
        {
            cout << "Logged out" << endl;
            close();
        }

        return;
    }

    if (cmd == "MESSAGE")
    {
        string dest = headerValue(frame, "destination");
        if (!dest.empty() && dest[0] == '/')
            dest = dest.substr(1);

        size_t pos = frame.find("\n\n");
        string body = (pos == string::npos) ? "" : frame.substr(pos + 2);

        auto blines = splitInput(body, '\n');

        string user, teamA, teamB, evName, desc;
        int t = 0;
        map<string, string> gen, aup, bup;

        enum Sec
        {
            NONE,
            GEN,
            A,
            B,
            DESC
        } sec = NONE;

        for (size_t i = 0; i < blines.size(); i++)
        {
            string line = trim(blines[i]);
            if (line.empty())
                continue;

            if (startsWith(line, "user:"))
            {
                user = trim(line.substr(5));
                continue;
            }
            if (startsWith(line, "team a:"))
            {
                teamA = trim(line.substr(7));
                continue;
            }
            if (startsWith(line, "team b:"))
            {
                teamB = trim(line.substr(7));
                continue;
            }
            if (startsWith(line, "event name:"))
            {
                evName = trim(line.substr(11));
                continue;
            }
            if (startsWith(line, "time:"))
            {
                t = std::stoi(trim(line.substr(5)));
                continue;
            }

            if (line == "general game updates:")
            {
                sec = GEN;
                continue;
            }
            if (line == "team a updates:")
            {
                sec = A;
                continue;
            }
            if (line == "team b updates:")
            {
                sec = B;
                continue;
            }
            if (line == "description:")
            {
                sec = DESC;
                desc.clear();
                for (size_t j = i + 1; j < blines.size(); j++)
                {
                    string d = blines[j];
                    if (!desc.empty())
                        desc += "\n";
                    desc += d;
                }
                break;
            }

            auto cpos = line.find(':');
            if (cpos != string::npos && sec != NONE && sec != DESC)
            {
                string k = trim(line.substr(0, cpos));
                string v = trim(line.substr(cpos + 1));
                if (sec == GEN)
                    gen[k] = v;
                if (sec == A)
                    aup[k] = v;
                if (sec == B)
                    bup[k] = v;
            }
        }

        if (user.empty())
            user = "unknown";
        string game = teamA + "_" + teamB;

        Event ev(teamA, teamB, evName, t, gen, aup, bup, desc);

        {
            std::lock_guard<std::mutex> lock(mtx);
            gameTeams[game] = {teamA, teamB};
            storeEventLocal(game, user, ev);
        }
        return;
    }

    // Server sent ERROR frame – print it and close connection
    if (cmd == "ERROR")
    {
        std::cout << frame << std::endl;
        close();
        return;
    }
}

void StompProtocol::close()
{
    std::lock_guard<std::mutex> lock(mtx);

    connected.store(false);
    username.clear();

    channelToId.clear();
    idToChannel.clear();
    receiptToAction.clear();

    if (handler)
    {
        handler->close();
        handler.reset();
    }
}
