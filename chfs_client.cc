// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE)
    {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR)
    {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    }
    printf("isfile: %lld is not a dir\n", inum);
    return false;
}

bool chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK)
    {
        printf("isfile: %lld is a symlink\n", inum);
        return true;
    }
    printf("isfile: %lld is not a symlink\n", inum);
    return false;
}

int chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        return r;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);
    return r;
}

int chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;

        return r;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

    return r;
}

#define EXT_RPC(xx)                                                \
    do                                                             \
    {                                                              \
        if ((xx) != extent_protocol::OK)                           \
        {                                                          \
            printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
            r = IOERR;                                             \
            goto release;                                          \
        }                                                          \
    } while (0)

// Only support set size of attr
int chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;
    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    extent_protocol::attr a;

    printf("SETATTR %016llx\n", ino);

    ec->getattr(ino, a);

    std::string buf;
    ec->get(ino, buf); // read file

    if (a.size < size)
        buf += std::string(size - a.size, '\0');
    else if (a.size > size)
        buf = buf.substr(0, size);
    ec->put(ino, buf);

    return r;
}

int chfs_client::addnew(inum parent, const char *name, mode_t mode, inum &ino_out, extent_protocol::types t)
{
    int r = OK;

    bool found = false;
    inum i;

    lookup(parent, name, found, i);
    if (found)
    {
        // has existed

        return EXIST;
    }
    else
    {
        // ec->begin();

        std::string buf;
        ec->create(t, ino_out);
        ec->get(parent, buf); // modify parent infomation
        buf.append(std::string(name) + ":" + filename(ino_out) + "/");
        ec->put(parent, buf);

        // ec->commit();

        return r;
    }
}

int chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    return addnew(parent, name, mode, ino_out, extent_protocol::T_FILE);
}

int chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    return addnew(parent, name, mode, ino_out, extent_protocol::T_DIR);
}

int chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    found = false;
    std::list<dirent> list;
    readdir(parent, list);
    if (list.empty())
        return r;
    for (std::list<dirent>::iterator it = list.begin(); it != list.end(); it++)
    {
        if (it->name == name)
        { // exist
            found = true;
            ino_out = it->inum;
            break;
        }
    }
    return r;
}

int chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    std::string buf;
    ec->get(dir, buf);

    if (!isdir(dir))
        return EXIST;

    int start = 0;
    int end = buf.find(':');
    while (end != std::string::npos)
    {
        std::string name = buf.substr(start, end - start); // get name

        int inum_start = end + 1; // get inum
        int inum_end = buf.find('/', inum_start);
        std::string inum = buf.substr(inum_start, inum_end - inum_start);

        struct dirent entry;
        entry.name = name;
        entry.inum = n2i(inum);
        list.push_back(entry);

        start = inum_end + 1;
        end = buf.find(':', start);
    }
    return r;
}

int chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */

    ec->get(ino, data);

    if (off > data.size())
        data = "";
    else
    {
        if (off + size <= data.size())
            data = data.substr(off, size);
        else
            data = data.substr(off, data.size() - off);
    }

    return r;
}

int chfs_client::write(inum ino, size_t size, off_t off, const char *data,
                       size_t &bytes_written)
{
    int r = OK;

    // ec->begin();

    std::string buf;
    ec->get(ino, buf);

    // off + write size <= file size
    if (off + size <= buf.size())
    {
        for (int i = off; i < off + size; i++)
        {
            buf[i] = data[i - off];
        }
        bytes_written = size;
        ec->put(ino, buf);
    }
    else
    {
        // off + write size > file size
        buf.resize(off + size);
        for (int i = off; i < off + size; i++)
        {
            buf[i] = data[i - off];
        }
        bytes_written = size;
        ec->put(ino, buf);
    }

    // ec->commit();

    return r;
}

int chfs_client::unlink(inum parent, const char *name)
{
    int r = OK;

    // ec->begin();

    bool found = false;
    inum inum;
    lookup(parent, name, found, inum);

    ec->remove(inum);

    std::string buf;
    ec->get(parent, buf);
    int start = buf.find(name);
    int after = buf.find('/', start);
    buf.erase(start, after - start + 1);
    ec->put(parent, buf);

    // ec->commit();

    return r;
}

int chfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out)
{
    int r = OK;

    // ec->begin();

    // r = addnew(parent, name, mode, ino_out, extent_protocol::T_SYMLINK);
    bool found = false;
    inum i;
    lookup(parent, name, found, i);
    if (found)
    {
        // has existed
        return EXIST;
    }
    else
    {
        std::string buf;

        ec->create(extent_protocol::T_SYMLINK, ino_out);
        ec->get(parent, buf); // modify parent infomation
        buf.append(std::string(name) + ":" + filename(ino_out) + "/");
        ec->put(parent, buf);
    }
    ec->put(ino_out, std::string(link));

    // ec->commit();

    return r;
}

int chfs_client::readlink(inum ino, std::string &data)
{
    int r = OK;
    ec->get(ino, data);
    return r;
}