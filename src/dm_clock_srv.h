#include <ostream>
#include <map>
#include <deque>
#include <mutex>


namespace dmc {


    struct RequestTag {
        double proportion;
        double reservation;
        double limit;

        RequestTag() :
            proportion(0), reservation(0), limit(0)
        {
            // empty
        }

        friend std::ostream& operator<<(std::ostream&, const RequestTag&);
    };


    struct ClientInfo {
        double weight;       // proportional
        double reservation;  // minimum
        double limit;        // maximum

        RequestTag prevTag;

        ClientInfo() : ClientInfo(-1.0, -1.0, -1.0) {}

        ClientInfo(double w, double r, double l) :
            weight(w),
            reservation(r),
            limit(l)
        {
            // empty
        }

        bool isUnset() const {
            return -1 == weight;
        }

        friend std::ostream& operator<<(std::ostream&, const ClientInfo&);
    };


    std::ostream& operator<<(std::ostream& out, const dmc::ClientInfo& client);
    std::ostream& operator<<(std::ostream& out, const dmc::RequestTag& tag);


    // T is client identifier type
    template<typename T>
    class ClientDB {

    protected:

        typename std::map<T,ClientInfo> map;

    public:

        // typedef std::map<T,ClientInfo>::const_iterator client_ref;

        // client_ref find(const T& clientId) const;
        ClientInfo find(const int& clientId) const {
            auto it = map.find(clientId);
            if (it == map.cend()) {
                return ClientInfo();
            } else {
                return it->second;
            }
        }
        
        void put(const T& clientId, const ClientInfo& info) {
            map[clientId] = info;
        }
    };

    
    template<typename T, typename R>
    class ClientQueue {

    public:

        typedef typename std::pair<RequestTag,R> Entry;

    protected:

        typedef typename std::lock_guard<std::mutex> Guard;

        std::deque<Entry> queue;
        mutable std::mutex queue_mutex;

    public:

        const Entry* peek_front() const {
            Guard g(queue_mutex);
            if (queue.empty()) {
                return NULL;
            } else {
                return &queue.front();
            }
        }

        // can only be called when queue is not empty
        void pop() {
            Guard g(queue_mutex);
            queue.pop_front();
        }

        void append(R request) {
            Entry entry(RequestTag(), request);
            std::lock_guard<std::mutex> guard(queue_mutex);
            queue.push_back(entry);
        }

        // can only be called when queue is not empty
        bool empty() const {
            Guard g(queue_mutex);
            return queue.empty();
        }


    }; // class ClientQueue

    
    // T is client identifier type, R is request type
    template<typename T, typename R>
    class PriorityQueue {

    }; // class PriorityQueue

    
} // namespace dmc
