#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <fstream>
#include <mutex>

template <typename command>
class raft_storage
{
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here
    int current_term = 0;
    std::vector<log_entry<command>> raft_log;

    void read_metadata();
    void read_log_entry();

    void write_metadata();
    void write_log_entry();
    void append_log_entry(log_entry<command>);

private:
    std::mutex mtx;
    std::string log_path;
    std::string meta_path;
    // Lab3: Your code here
};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir)
{
    // Lab3: Your code here
    log_path = dir + "/log";
    meta_path = dir + "/meta";
    read_metadata();
    read_log_entry();
}

template <typename command>
raft_storage<command>::~raft_storage()
{
    // Lab3: Your code here
    write_metadata();
    write_log_entry();
}

template <typename command>
void raft_storage<command>::read_metadata()
{
    std::ifstream infile(meta_path, std::ios::in | std::ios::binary);
    if (infile.is_open())
        infile.read((char *)&current_term, sizeof(int));
    infile.close();

    return;
}

template <typename command>
void raft_storage<command>::read_log_entry()
{
    std::ifstream infile(log_path, std::ios::in | std::ios::binary);
    if (infile.is_open())
    {
        while (infile.peek() != EOF)
        {
            log_entry<command> new_entry;

            int size;
            char *buf;
            infile.read((char *)&new_entry.term, sizeof(int));
            infile.read((char *)&size, sizeof(int));

            buf = new char[size];
            infile.read(buf, size);

            new_entry.cmd.deserialize(buf, size);
            raft_log.push_back(new_entry);
            delete[] buf;
        }
    }
    infile.close();
    return;
}

template <typename command>
void raft_storage<command>::write_metadata()
{
    mtx.lock();
    std::ofstream outfile(meta_path, std::ios::trunc | std::ios::out | std::ios::binary); // reopen a file
    if (outfile.is_open())
        outfile.write((char *)&current_term, sizeof(int));
    outfile.close();
    mtx.unlock();
    return;
}

template <typename command>
void raft_storage<command>::write_log_entry()
{
    mtx.lock();
    std::ofstream outfile(log_path, std::ios::trunc | std::ios::out | std::ios::binary); // reopen a file
    if (outfile.is_open())
    {
        for (auto &log : raft_log)
        {
            int size = log.cmd.size();
            char *buf = new char[size];
            log.cmd.serialize(buf, size);

            outfile.write((char *)&log.term, sizeof(int));
            outfile.write((char *)&size, sizeof(int));
            outfile.write(buf, size);
            delete[] buf;
        }
    }
    outfile.close();
    mtx.unlock();
    return;
}

template <typename command>
void raft_storage<command>::append_log_entry(log_entry<command> log)
{
    mtx.lock();
    std::ofstream outfile(log_path, std::ios::app | std::ios::out | std::ios::binary); // reopen a file
    if (outfile.is_open())
    {
        int size = log.cmd.size();
        char *buf = new char[size];
        log.cmd.serialize(buf, size);

        outfile.write((char *)&log.term, sizeof(int));
        outfile.write((char *)&size, sizeof(int));
        outfile.write(buf, size);
        delete[] buf;
    }
    outfile.close();
    mtx.unlock();
    return;
}

#endif // raft_storage_h