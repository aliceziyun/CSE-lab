#include "extent_server_dist.h"

chfs_raft *extent_server_dist::leader() const
{
    // printf("leader\n");
    int leader = this->raft_group->check_exact_one_leader();
    if (leader < 0)
    {
        return this->raft_group->nodes[0];
    }
    else
    {
        return this->raft_group->nodes[leader];
    }
}

int extent_server_dist::create(uint32_t type, extent_protocol::extentid_t &id)
{
    // Lab3: your code here
    // printf("you need to create %lld\n",id);
    chfs_command_raft cmd(chfs_command_raft::CMD_CRT, type, id, "");
    chfs_raft *do_leader = leader();
    int term, index;
    do_leader->new_command(cmd, term, index);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!cmd.res->done)
    {
        ASSERT(
            cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(10000)) == std::cv_status::no_timeout,
            "extent_server_dist::create command timeout");
    }
    id = cmd.res->id;

    return extent_protocol::OK;
}

int extent_server_dist::put(extent_protocol::extentid_t id, std::string buf, int &)
{
    // Lab3: your code here
    // printf("you need to put\n");
    int type = 0;
    chfs_command_raft cmd(chfs_command_raft::CMD_PUT, type, id, buf);

    chfs_raft *do_leader = leader();
    int term, index;
    do_leader->new_command(cmd, term, index);

    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!cmd.res->done)
    {
        ASSERT(
            cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(10000)) == std::cv_status::no_timeout,
            "extent_server_dist::put command timeout");
    }
    lock.unlock();
    printf("successfully put\n");
    return extent_protocol::OK;
}

int extent_server_dist::get(extent_protocol::extentid_t id, std::string &buf)
{
    // Lab3: your code here
    // printf("you need to get\n");
    int type = 0;
    chfs_command_raft cmd(chfs_command_raft::CMD_GET, type, id, buf);
    chfs_raft *do_leader = leader();
    int term, index;
    do_leader->new_command(cmd, term, index);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!cmd.res->done)
    {
        ASSERT(
            cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(10000)) == std::cv_status::no_timeout,
            "extent_server_dist::get command timeout");
    }
    buf = cmd.res->buf;
    return extent_protocol::OK;
}

int extent_server_dist::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
    // Lab3: your code here
    int type;
    chfs_command_raft cmd(chfs_command_raft::CMD_GETA, type, id, "");
    chfs_raft *do_leader = leader();
    int term, index;
    do_leader->new_command(cmd, term, index);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!cmd.res->done)
    {
        ASSERT(
            cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(10000)) == std::cv_status::no_timeout,
            "extent_server_dist::getattr command timeout");
    }
    a = cmd.res->attr;
    // printf("successfully geta\n");
    return extent_protocol::OK;
}

int extent_server_dist::remove(extent_protocol::extentid_t id, int &)
{
    // Lab3: your code here
    // printf("you need to remove\n");
    int type = 0;
    chfs_command_raft cmd(chfs_command_raft::CMD_RMV, type, id, "");
    chfs_raft *do_leader = leader();
    int term, index;
    do_leader->new_command(cmd, term, index);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!cmd.res->done)
    {
        ASSERT(
            cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(10000)) == std::cv_status::no_timeout,
            "extent_server_dist::remove command timeout");
    }
    // printf("successfully remove\n");
    return extent_protocol::OK;
}

extent_server_dist::~extent_server_dist()
{
    delete this->raft_group;
}