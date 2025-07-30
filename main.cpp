using namespace std;
#include<bits/stdc++.h>
struct MBORecord{
    string ts_recv;
    string ts_event;
    int rtype;
    int publisher_id;
    int instrument_id;
    char action;
    char side;
    double price;
    long long size;
    int channel_id;
    long long order_id;
    int flags;
    long long ts_in_delta;
    long long sequence;
    string symbol;
};
class OrderBook{
private:
map<double, vector<MBORecord>, greater<double>> bids;
map<double, vector<MBORecord>> asks;
unordered_map<long long, pair<double, char>> order_info;

public:
void Apply(const MBORecord& record){
    switch(record.action){
        case 'A':
        Add(record);
        break;
        case 'C':
        Cancel(record);
        break;
        case 'T':
        if(record.side!='N'){
            HandleTrade(record);
        }
        break;
        case 'F':
        break;
        default:
        break;
    }
}
void Add(const MBORecord&record){
    order_info[record.order_id]={record.price, record.side};
    auto &level=(record.side=='B')?bids[record.price]:asks[record.price];
    level.push_back(record);

}
void Cancel(const MBORecord&record){
    auto it=order_info.find(record.order_id);
    if(it==order_info.end()) return;
    double price=it->second.first;
    char side=it->second.second;
    auto &level=(side=='B')?bids[price]:asks[price];
    auto order_it=find_if(level.begin(),level.end(),[&](const MBORecord& r) {
        return r.order_id == record.order_id;
    });
    if(order_it!=level.end()){
        order_it->size-=record.size;
        if(order_it->size<=0){
            level.erase(order_it);
            if(level.empty()){
                if(side=='B'){
                    bids.erase(price);
                }
                else{
                    asks.erase(price);
                }
            }
        }
        order_info.erase(it);
    }

}
void HandleTrade(const MBORecord&record){
    auto& level = (record.side == 'B') ? bids[record.price] : asks[record.price];
    if (level.empty()) return;

    auto& front_order = level.front();
    front_order.size -= record.size;
    if (front_order.size <= 0) {
        order_info.erase(front_order.order_id);
        level.erase(level.begin());
        if (level.empty()) {
            if (record.side == 'B') bids.erase(record.price);
            else asks.erase(record.price);
        }
    }
}

    void OutputMBP10(ofstream& output, const MBORecord& record) {
    int depth = max((int)bids.size(), (int)asks.size());
    if (depth > 10) depth = 10;

    output << record.ts_recv << "," << record.ts_event << "," << record.rtype << ","
           << record.publisher_id << "," << record.instrument_id << ","
           << record.action << "," << record.side << "," << depth << ",";

    auto bid_it = bids.begin();
    auto ask_it = asks.begin();

    for (int i = 0; i < 10; ++i) {
        // --- Bids ---
        if (bid_it != bids.end()) {
            long long total_size = 0;
            int order_count = 0;
            for (const auto& order : bid_it->second) {
                total_size += order.size;
                order_count++;
            }
            output << fixed << setprecision(2) << bid_it->first << "," << total_size << "," << order_count << ",";
            ++bid_it;
        } else {
            output << "0,0,0,";
        }

        // --- Asks ---
        if (ask_it != asks.end()) {
            long long total_size = 0;
            int order_count = 0;
            for (const auto& order : ask_it->second) {
                total_size += order.size;
                order_count++;
            }
            output << fixed << setprecision(2) << ask_it->first << "," << total_size << "," << order_count << ",";
            ++ask_it;
        } else {
            output << "0,0,0,";
        }
    }

    output << record.symbol << "," << record.order_id << "\n";
}

};




class CSVParser{
    public:
     static vector<string> split(const string& line, char delimiter) {
        vector<string> tokens;
        stringstream ss(line);
        string token;
        
        while (getline(ss, token, delimiter)) {
            tokens.push_back(token);
        }
        return tokens;
    }
    static MBORecord parseMBOLine(const string&line){
        auto tokens=split(line,',');
        MBORecord record;
         record.ts_recv = tokens[0];
            record.ts_event = tokens[1];
            record.rtype = std::stoi(tokens[2]);
            record.publisher_id = std::stoi(tokens[3]);
            record.instrument_id = std::stoi(tokens[4]);
            record.action = tokens[5][0];
            record.side = tokens[6][0];
             try {
        record.price = std::stod(tokens[7]);
    } catch (const std::invalid_argument&) {
        
        record.price = 0.0;
    }
            record.size = std::stoll(tokens[8]);
            record.channel_id = std::stoi(tokens[9]);
            record.order_id = std::stoll(tokens[10]);
            record.flags = std::stoi(tokens[11]);
            record.ts_in_delta = std::stoll(tokens[12]);
            record.sequence = std::stoll(tokens[13]);
            record.symbol = tokens[14];
        return record;
    }
};
int main(int argc,char *argv[]){
    if(argc!=2){
        cerr<<"Usage: "<<argv[0]<<" <mbo.csv>"<<endl;
    }
    ifstream input(argv[1]);
    if(!input.is_open()){
        cerr<<"Error: Could not open file "<<argv[1]<<endl;
        return 1;
    }
    ofstream output("mbp.csv");
    if(!output.is_open()){
        cerr<<"Error: Could not open output file mbo.csv"<<endl;
        return 1;
    }
    output << "ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,depth,price,size,flags,ts_in_delta,sequence,"
           << "bid_px_00,bid_sz_00,bid_ct_00,ask_px_00,ask_sz_00,ask_ct_00,"
           << "bid_px_01,bid_sz_01,bid_ct_01,ask_px_01,ask_sz_01,ask_ct_01,"
           << "bid_px_02,bid_sz_02,bid_ct_02,ask_px_02,ask_sz_02,ask_ct_02,"
           << "bid_px_03,bid_sz_03,bid_ct_03,ask_px_03,ask_sz_03,ask_ct_03,"
           << "bid_px_04,bid_sz_04,bid_ct_04,ask_px_04,ask_sz_04,ask_ct_04,"
           << "bid_px_05,bid_sz_05,bid_ct_05,ask_px_05,ask_sz_05,ask_ct_05,"
           << "bid_px_06,bid_sz_06,bid_ct_06,ask_px_06,ask_sz_06,ask_ct_06,"
           << "bid_px_07,bid_sz_07,bid_ct_07,ask_px_07,ask_sz_07,ask_ct_07,"
           << "bid_px_08,bid_sz_08,bid_ct_08,ask_px_08,ask_sz_08,ask_ct_08,"
           << "bid_px_09,bid_sz_09,bid_ct_09,ask_px_09,ask_sz_09,ask_ct_09,"
           << "symbol,order_id" << endl;
    OrderBook orderbook;
    string line;
    bool first_line=true;
    getline(input,line);
    int processed=0;
    while(getline(input,line)){
        MBORecord record=CSVParser::parseMBOLine(line);
        if(record.action=='R'){
            orderbook.OutputMBP10(output,record);
            continue;
        }
       orderbook.Apply(record);
       orderbook.OutputMBP10(output,record);
       processed++;
       if(processed%1000==0){
        cout<<"Processed "<<processed<<" records."<<endl;
       }



    }
    input.close();
    output.close();
    cout<<"Order book processing completed."<<endl;
    return 0;

}