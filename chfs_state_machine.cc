#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft()
{
    // Lab3: Your code here
}

chfs_command_raft::chfs_command_raft(command_type tp, uint32_t type, extent_protocol::extentid_t id, std::string buf) : cmd_tp(tp), type(type), id(id), buf(buf)
{
    res=std::make_shared<result>();
    res->tp = cmd_tp;
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) : cmd_tp(cmd.cmd_tp), type(cmd.type), id(cmd.id), buf(cmd.buf), res(cmd.res)
{
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft()
{
    // Lab3: Your code here
}

int chfs_command_raft::size() const
{
    // Lab3: Your code here
    return sizeof(command_type) + buf.size() + sizeof(id) + sizeof(type) + sizeof(int);
    return 0;
}

void chfs_command_raft::serialize(char *buf_out, int size) const
{
    // Lab3: Your code here
    if (size != this->size())
        return;
    int buf_size = buf.size();
    int pos = 0;

    memcpy(buf_out + pos, (char *)&cmd_tp, sizeof(command_type));
    pos += sizeof(command_type);
    memcpy(buf_out + pos, (char *)&id, sizeof(id));
    pos += sizeof(id);
    memcpy(buf_out + pos, (char *)&type, sizeof(type));
    pos += sizeof(type);
    memcpy(buf_out + pos, (char *)&buf_size, sizeof(int)); //记录buf的大小
    pos += sizeof(int);
    memcpy(buf_out + pos, buf.c_str(), buf_size);
    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size)
{
    // Lab3: Your code here
    int buf_size;
    int pos = 0;

    memcpy((char *)&cmd_tp, buf_in + pos, sizeof(command_type));
    pos += sizeof(command_type);
    memcpy((char *)&id, buf_in + pos, sizeof(id));
    pos += sizeof(id);
    memcpy((char *)&type, buf_in + pos, sizeof(type));
    pos += sizeof(type);
    memcpy((char *)&buf_size, buf_in + pos, sizeof(int));
    pos += sizeof(int);
    buf = std::string(buf_in + pos, buf_size);
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd)
{
    // Lab3: Your code here
    m << (int)cmd.cmd_tp << cmd.id << cmd.type << cmd.buf;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd)
{
    // Lab3: Your code here
    int type;
    u >> type >> cmd.id >> cmd.type >> cmd.buf;
    cmd.cmd_tp = chfs_command_raft::command_type(type);
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd)
{
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    std::unique_lock<std::mutex> lock(mtx);
    // Lab3: Your code here
    int tmp = 0;
    bool ptrflag = false;
    if (chfs_cmd.res == nullptr){
        ptrflag = true;
    }

    if (ptrflag)
    {
        chfs_cmd.res = std::make_shared<chfs_command_raft::result>();
    }

    switch (chfs_cmd.cmd_tp)
    {
    case chfs_command_raft::command_type::CMD_CRT:
        es.create(chfs_cmd.type, chfs_cmd.res->id);
        break;
    case chfs_command_raft::command_type::CMD_GET:
        es.get(chfs_cmd.id, chfs_cmd.res->buf);
        break;
    case chfs_command_raft::command_type::CMD_GETA:
        es.getattr(chfs_cmd.id, chfs_cmd.res->attr);
        break;
    case chfs_command_raft::command_type::CMD_RMV:
        es.remove(chfs_cmd.id, tmp);
        break;
    case chfs_command_raft::CMD_PUT:
        es.put(chfs_cmd.id, chfs_cmd.buf, tmp);
        break;
    default:
        printf("default case\n");
        exit(0);
        break;
    }
    if (!ptrflag)
    {
        chfs_cmd.res->done = true;
        chfs_cmd.res->cv.notify_all();
    }

    // printf("apply_log successfuly\n");

    lock.unlock();
    return;
}
