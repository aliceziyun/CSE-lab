#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <random>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template <typename state_machine, typename command>
class raft
{
    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

    // #define RAFT_LOG(fmt, args...) \
//     do                         \
//     {                          \
//     } while (0);

#define RAFT_LOG(fmt, args...)                                                                                   \
    do                                                                                                           \
    {                                                                                                            \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while (0);

public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    int commit_index;
    int apply_index; // the log last applied

    std::atomic_bool stopped;

    enum raft_role
    {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;
    int votes = 0;
    std::clock_t last_received_RPC_time;

    // std::vector<log_entry<command>> storage->raft_log; // logs on one machine
    std::vector<int> next_index;    // record the size of log in each machine
    std::vector<int> matched_index; // record the matched index of each machine

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:
    void begin_new_term(int term);
    bool check_log_consistency(int prev_log_index, int prev_log_term);
    bool vote_for_candidate(int index, int term);
    int marjority_of_all();

    /* ----Persistent state on all server----  */

    /* ---- Volatile state on all server----  */

    /* ---- Volatile state on leader----  */

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes()
    {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) : stopped(false),
                                                                                                                                               rpc_server(server),
                                                                                                                                               rpc_clients(clients),
                                                                                                                                               my_id(idx),
                                                                                                                                               storage(storage),
                                                                                                                                               state(state),
                                                                                                                                               background_election(nullptr),
                                                                                                                                               background_ping(nullptr),
                                                                                                                                               background_commit(nullptr),
                                                                                                                                               background_apply(nullptr),
                                                                                                                                               current_term(0),
                                                                                                                                               role(follower)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    commit_index = 0;
    apply_index = 0;
    last_received_RPC_time = clock();

    current_term = storage->current_term;
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft()
{
    if (background_ping)
    {
        delete background_ping;
    }
    if (background_election)
    {
        delete background_election;
    }
    if (background_commit)
    {
        delete background_commit;
    }
    if (background_apply)
    {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop()
{
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped()
{
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term)
{
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start()
{
    // Lab3: Your code here
    RAFT_LOG("EEEE start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index)
{
    // Lab3: Your code here
    mtx.lock();

    if (role == leader)
    {
        log_entry<command> new_log;
        new_log.cmd = cmd;
        new_log.term = current_term;
        storage->raft_log.push_back(new_log);
        storage->append_log_entry(new_log);
        RAFT_LOG("RECEIVE NEW COMMAND %d", storage->raft_log.size());

        term = current_term;
        index = (int)storage->raft_log.size();

        mtx.unlock();
        return true;
    }

    mtx.unlock();
    return false;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot()
{
    // Lab3: Your code here
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply)
{
    // Lab3: Your code here
    mtx.lock(); // big lock

    RAFT_LOG("Request FROM %d, TERM %d", args.candidateId, args.term);

    if (args.term < current_term)
    {
        reply.term = current_term;
        reply.voteGranted = false; // no voting
    }
    else
    {
        if (args.term > current_term)
        {
            // change the role
            if (role == leader || role == candidate)
                role = follower;
            begin_new_term(args.term);
        }
        // judge whether should vote to the candidate
        bool voting = vote_for_candidate(args.lastLogIndex, args.lastLogTerm);
        if (voting)
        {
            reply.term = args.term;
            reply.voteGranted = true;
            mtx.unlock();
            return 0;
        }
    }
    reply.voteGranted = false;
    reply.term = current_term;
    mtx.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply)
{
    // Lab3: Your code here
    mtx.lock();
    if (role == follower)
    {
        mtx.unlock();
        return;
    }
    if (reply.term > current_term)
    {
        if (role == leader || role == candidate)
            role = follower;
        begin_new_term(reply.term);
    }
    else
    {
        if (role != candidate)
        {
            mtx.unlock();
            return;
        }
        if (reply.voteGranted == true)
            votes++;
        if (votes > num_nodes() / 2)
        {
            role = leader;
            next_index = std::vector<int>(num_nodes(), storage->raft_log.size()); // set next_index to every log size()
            matched_index = std::vector<int>(num_nodes(), 0);                     // set match_index to 0
            RAFT_LOG("become leader %d", my_id);
        }
    }
    mtx.unlock();
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply)
{
    // Lab3: Your code here
    mtx.lock();

    bool consistent = check_log_consistency(arg.prevLogIndex, arg.prevLogTerm);

    // ping
    if (arg.entries.empty())
    {
        last_received_RPC_time = clock(); // update response time
        if (role == candidate)            // cancel candidates
            role = follower;
        if (consistent)
        {
            if (arg.leaderCommit > commit_index)
            {
                commit_index = std::min(arg.leaderCommit, (int)(storage->raft_log.size()));
                // RAFT_LOG("current commit index %d", commit_index);
            }
        }
        mtx.unlock();
        return 0;
    }

    // invalid log
    if (arg.term < current_term)
    {
        RAFT_LOG("invalid log %d", arg.term);
        reply.success = false;
        reply.term = current_term;
        mtx.unlock();
        return 0;
    }

    if (current_term < arg.term)
    {
        if (role == candidate || role == leader)
        {
            RAFT_LOG("back to follower in log append");
            role = follower;
        }
        RAFT_LOG("begin new term in append");
        begin_new_term(arg.term);
    }

    // inconsistent
    if (!consistent)
    {
        // RAFT_LOG("inconsistent!");
        reply.success = false;
        reply.term = current_term;
        mtx.unlock();
        return 0;
    }

    reply.success = true;
    // vector is really fangbian
    storage->raft_log.erase(storage->raft_log.begin() + arg.prevLogIndex + 1, storage->raft_log.end());
    storage->raft_log.insert(storage->raft_log.end(), arg.entries.begin(), arg.entries.end());
    // storage->write_log_entry();

    if (arg.leaderCommit > commit_index)
        commit_index = std::min(arg.leaderCommit, (int)(storage->raft_log.size() - 1));
    // RAFT_LOG("consistent,apply to %d with log len %d", my_id, (int)storage->raft_log.size());
    mtx.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply)
{
    // Lab3: Your code here
    mtx.lock();

    if (role != leader)
    {
        mtx.unlock();
        return;
    }

    // PING
    if (arg.entries.empty())
    {
        mtx.unlock();
        return;
    }

    if (!reply.success)
    {
        if (reply.term > current_term)
        {
            if (role == leader || role == candidate)
                role = follower;
            RAFT_LOG("back to follower");
            begin_new_term(reply.term);
            mtx.unlock();
            return;
        }
        else if (next_index[node] > 0)
            next_index[node]--; // cut the next index with 1

        mtx.unlock();
        return;
    }
    else
    {
        matched_index[node] = arg.prevLogIndex + arg.entries.size();
        next_index[node] = matched_index[node] + 1; // resize the next index with matched index
        commit_index = marjority_of_all() + 1;
        // RAFT_LOG("current commit_index %d", commit_index);

        mtx.unlock();
        return;
    }
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply)
{
    // Lab3: Your code here
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply)
{
    // Lab3: Your code here
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg)
{
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0)
    {
        handle_request_vote_reply(target, arg, reply);
    }
    else
    {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg)
{
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0)
    {
        handle_append_entries_reply(target, arg, reply);
    }
    else
    {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg)
{
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0)
    {
        handle_install_snapshot_reply(target, arg, reply);
    }
    else
    {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election()
{
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.
    while (true)
    {
        bool start_election = false;
        if (is_stopped())
            return;
        // Lab3: Your code here
        mtx.lock();
        // work for candidates and followers
        if (role != leader)
        {
            std::random_device dev;
            std::mt19937 rng(dev());
            std::uniform_real_distribution<double> d(0.1, 0.3);

            double timeout1 = d(rng);
            // double timeout2 = 0.05;
            double delta_time = (double)(clock() - last_received_RPC_time) / CLOCKS_PER_SEC;
            // RAFT_LOG("prepare election %f", delta_time);
            if (role == follower)
                role = candidate;
            if (role == candidate)
            {
                // if (current_term == 0)
                // {
                if (delta_time > timeout1)
                {
                    RAFT_LOG("election start2 %f", delta_time);
                    start_election = true;
                }
                // }
                // else if (delta_time > timeout1 + timeout2)
                // {
                //     RAFT_LOG("election start2 %f", delta_time);
                //     start_election = true;
                // }
            }
        }
        if (start_election)
        {
            begin_new_term(current_term + 1);

            // send message to all servers
            request_vote_args args;
            args.term = current_term;
            args.candidateId = my_id;
            args.lastLogIndex = storage->raft_log.size() - 1;
            if (args.lastLogIndex == -1)
                args.lastLogTerm = current_term;
            else
                args.lastLogTerm = storage->raft_log[args.lastLogIndex].term;
            for (int i = 0; i < num_nodes(); ++i)
            {
                // RAFT_LOG("send elect message to %d", i);
                if (!thread_pool->addObjJob(this, &raft::send_request_vote, i, args))
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); // sleep for a while
    }
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit()
{
    // Periodly send logs to the follower.

    // Only work for the leader.

    while (true)
    {
        if (is_stopped())
            return;
        // Lab3: Your code here
        mtx.lock();
        if (role == leader)
        {
            for (int i = 0; i < num_nodes(); ++i)
            {
                if (storage->raft_log.size() == 0)
                    break;
                if (i == my_id)
                {
                    matched_index[i] = storage->raft_log.size() - 1;
                    continue;
                }
                if (storage->raft_log.size() < next_index[i] + 1)
                    continue;
                // RAFT_LOG("commit start to %d,with log size %d", i, (int)storage->raft_log.size());
                append_entries_args<command> args;

                int index = next_index[i];
                args.term = current_term;
                args.leaderId = my_id;
                args.leaderCommit = commit_index;
                args.prevLogIndex = index - 1;
                args.entries = std::vector<log_entry<command>>(storage->raft_log.begin() + index, storage->raft_log.end());
                if (index == 0)
                {
                    args.prevLogTerm = current_term;
                }
                else
                {
                    args.prevLogTerm = storage->raft_log[index].term;
                }

                if (!thread_pool->addObjJob(this, &raft::send_append_entries, i, args))
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply()
{
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    while (true)
    {
        if (is_stopped())
            return;
        // Lab3: Your code here:
        mtx.lock();
        storage->write_log_entry();
        while (commit_index > apply_index)
        {
            // RAFT_LOG("apply %d %d on %d,with log len %d", commit_index, apply_index, my_id, (int)storage->raft_log.size());
            state->apply_log(storage->raft_log[apply_index].cmd);
            ++apply_index;
        }
        // RAFT_LOG("apply done");
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping()
{
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    while (true)
    {
        if (is_stopped())
            return;
        // Lab3: Your code here:
        mtx.lock();
        if (role == leader) // send empty log entries to all nodes
        {
            for (int i = 0; i < num_nodes(); ++i)
            {
                // RAFT_LOG("send ping to %d", i);
                if (i == my_id)
                    continue;

                int index = next_index[i] - 1;
                append_entries_args<command> empty_args;
                empty_args.term = current_term;
                empty_args.leaderId = my_id;
                empty_args.leaderCommit = commit_index;
                empty_args.prevLogIndex = index;
                if (index == -1)
                {
                    empty_args.prevLogTerm = current_term;
                }
                else
                {
                    empty_args.prevLogTerm = storage->raft_log[index].term;
                }
                empty_args.entries.clear();

                thread_pool->addObjJob(this, &raft::send_append_entries, i, empty_args);
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }

    return;
}

/******************************************************************

                        Other functions

*******************************************************************/
template <typename state_machine, typename command>
void raft<state_machine, command>::begin_new_term(int term)
{
    votes = 0;
    current_term = term;
    last_received_RPC_time = clock();

    storage->current_term = term;
    storage->write_metadata();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::check_log_consistency(int prev_log_index, int prev_log_term)
{
    if (prev_log_index == -1 && prev_log_term == current_term)
        return true;
    if ((prev_log_index == (int)(storage->raft_log.size() - 1)) && (storage->raft_log[prev_log_index].term == prev_log_term))
        return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::vote_for_candidate(int index, int term)
{
    int largest_index = (int)storage->raft_log.size() - 1;
    int largest_term = 0;
    if (largest_index != -1)
        largest_term = storage->raft_log[largest_index].term;

    if (largest_term < term)
        return true;
    else if ((largest_term == term) && (largest_index <= index))
        return true;
    return false;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::marjority_of_all()
{
    std::vector<int> tmp = matched_index;
    std::sort(tmp.begin(), tmp.end()); // sort
    return tmp[tmp.size() / 2];        // majority
}

#endif // raft_h