#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}


void
disk::read_block(blockid_t id, char *buf)
{
  if(id >= 0 && id < BLOCK_NUM && buf != NULL)
  {
    memcpy(buf, blocks[id], BLOCK_SIZE);
  }
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if(id >= 0 && id < BLOCK_NUM && buf != NULL)
  {
    memcpy(blocks[id], buf, BLOCK_SIZE);
  }
}




// block layer -----------------------------------------



//part1b

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  blockid_t currentBlock = IBLOCK(INODE_NUM, BLOCK_NUM) + 1;
  for(; currentBlock < BLOCK_NUM; ++currentBlock)
  {
    if(using_blocks[currentBlock] == 0)
    {
      using_blocks[currentBlock] = 1;
      return currentBlock;
    }
  }
  return -1;
}

//part1b

void
block_manager::free_block(uint32_t id)
{  
  using_blocks[id] = 0;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

//part1a

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  char *buf = (char *)malloc(BLOCK_SIZE);
  int i = 0;
  int startIndex = IBLOCK(0, BLOCK_NUM);
  for(i = 0; i < INODE_NUM / IPB; ++i) 
  {
    int currentBlock = startIndex + i;
	  bm->read_block(currentBlock, buf);
	  for(int j = 0; j < IPB; ++j) 
    {
		  if(i * IPB + j == 0) 
      {
			  continue;
		  }
		  struct inode *node = (struct inode *)buf + j;
		  if(!node->type) 
      {    
		  	node->type = type;
        node->size = 0;
		  	bm->write_block(currentBlock, buf);
		  	free(buf);
		  	return i * IPB + j;
		  }
	  }
  }
  return 1;

}

//part1c
void
inode_manager::free_inode(uint32_t inum)
{
  struct inode* currentInode = get_inode(inum);
  //inode free
  if(currentInode == NULL || currentInode -> type == 0)
  {
    return;
  }
  //inode not free
  currentInode -> type = 0;
  put_inode(inum, currentInode);
  free(currentInode);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
   printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))



//part1b
/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  struct inode* currentInode = get_inode(inum);
  (*size) = currentInode -> size;
  if(!currentInode -> size) //empty
  {
    return;
  }
  else if(currentInode -> size > MAXFILE * BLOCK_SIZE)//oversize
  {
    exit(0);
  }
  else
  {
    uint32_t blockCount = (currentInode -> size + BLOCK_SIZE -1) / BLOCK_SIZE;
    char *buffer = (char *)malloc(blockCount *BLOCK_SIZE);
    if(blockCount <= NDIRECT)
    {
      for(uint32_t i = 0; i < blockCount; ++i)
      {
        bm->read_block(currentInode -> blocks[i], buffer + i * BLOCK_SIZE);
      }
    }
    else if(blockCount <= NDIRECT + NINDIRECT)
    {
      for(uint32_t i = 0; i < NDIRECT; ++i)
      {
        bm->read_block(currentInode -> blocks[i], buffer + i * BLOCK_SIZE);
      }
      blockid_t *indirectBlockIndex = (blockid_t*)malloc(BLOCK_SIZE);
      bm->read_block(currentInode -> blocks[NDIRECT], (char*)indirectBlockIndex);
      for(uint32_t i = 0; i < blockCount - NDIRECT; ++i)
      {
        bm->read_block(indirectBlockIndex[i], buffer + (i + NDIRECT) * BLOCK_SIZE);
      }
      free(indirectBlockIndex);
    }
    (*buf_out) = buffer;
  }
  free(currentInode);
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  struct inode* currentInode = get_inode(inum);
  uint32_t blockCountBefore = (currentInode -> size + BLOCK_SIZE -1) / BLOCK_SIZE;
  uint32_t blockCountAfter = (size + BLOCK_SIZE -1) / BLOCK_SIZE;
  if(blockCountAfter > NDIRECT + NINDIRECT)
  {
    exit(0);
  }
  blockid_t *indirectBlockIndex = NULL;

  if(blockCountAfter < blockCountBefore)
  {
    if(blockCountBefore <= NDIRECT)
    {
      for(uint32_t i = blockCountAfter; i < blockCountBefore; ++i)
      {
        bm -> free_block(currentInode -> blocks[i]);
      }
    }
    else if(blockCountAfter > NDIRECT)
    {
      indirectBlockIndex = (blockid_t*)malloc(BLOCK_SIZE);
      bm->read_block(currentInode -> blocks[NDIRECT], (char*)indirectBlockIndex);
      for(uint32_t i = blockCountAfter - NDIRECT; i < blockCountBefore - NDIRECT; ++i)
      {
        bm -> free_block(indirectBlockIndex[i]);
      }
    }
    else
    {
      indirectBlockIndex = (blockid_t*)malloc(BLOCK_SIZE);
      bm->read_block(currentInode -> blocks[NDIRECT], (char*)indirectBlockIndex);
      for(uint32_t i = 0; i < blockCountBefore - NDIRECT; ++i)
      {
        bm -> free_block(indirectBlockIndex[i]);
      }
      for(uint32_t i = blockCountAfter; i <= NDIRECT; ++i)
      {
        bm -> free_block(currentInode -> blocks[i]);
      }
    }
  }
  else
  {
    if(blockCountAfter <= NDIRECT)
    {
      for(uint32_t i = blockCountBefore; i < blockCountAfter; ++i)
      {
        currentInode -> blocks[i] = bm -> alloc_block();
      }
    }
    else if(blockCountBefore > NDIRECT)
    {
      indirectBlockIndex = (blockid_t*)malloc(BLOCK_SIZE);
      bm->read_block(currentInode -> blocks[NDIRECT], (char*)indirectBlockIndex);
      for(uint32_t i = blockCountBefore - NDIRECT; i < blockCountAfter - NDIRECT; ++i)
      {
        indirectBlockIndex[i] = bm->alloc_block();
      }
      bm -> write_block(currentInode -> blocks[NDIRECT], (char *)indirectBlockIndex);
    }
    else
    {
      for(uint32_t i = blockCountBefore; i < NDIRECT; ++i)
      {
        currentInode -> blocks[i] = bm -> alloc_block();
      }
      indirectBlockIndex = (blockid_t*)malloc(BLOCK_SIZE);
      for(uint32_t i = 0; i < blockCountAfter - NDIRECT; ++i)
      {
        indirectBlockIndex[i] = bm -> alloc_block();
      }
      blockid_t temp = bm -> alloc_block();
      currentInode -> blocks[NDIRECT] = temp;
      bm ->write_block(currentInode -> blocks[NDIRECT], (char*)indirectBlockIndex);
    }
  }

  

  //写入data
  if(blockCountAfter <= NDIRECT)
  {
    uint32_t i = 0;
    for(; i + 1 < blockCountAfter; ++i)
    {
      bm -> write_block(currentInode -> blocks[i], buf + BLOCK_SIZE * i);
    }
    char *lastBlock = (char*)malloc(BLOCK_SIZE);
    memcpy(lastBlock, buf + BLOCK_SIZE * (i), ((size - 1 + BLOCK_SIZE) % BLOCK_SIZE + 1));
    bm -> write_block(currentInode -> blocks[i], lastBlock);
    free(lastBlock);
  }
  else
  {
    
    uint32_t i = 0;
    for(; i < NDIRECT; ++i)
    {
      bm -> write_block(currentInode -> blocks[i], buf + BLOCK_SIZE * i);
    }
    for(i = 0; i < blockCountAfter - NDIRECT - 1; ++i)
    {
      bm -> write_block(indirectBlockIndex[i], buf + BLOCK_SIZE * (i + NDIRECT));
    }
    char *lastBlock = (char*)malloc(BLOCK_SIZE);
    memcpy(lastBlock, buf + BLOCK_SIZE * (i + NDIRECT), ((size - 1 + BLOCK_SIZE) % BLOCK_SIZE + 1));
    bm -> write_block(indirectBlockIndex[i], lastBlock);
    free(lastBlock);
  }
  
  currentInode -> size = size;
  currentInode -> mtime = time(0);
  currentInode -> ctime = currentInode -> mtime;
  put_inode(inum, currentInode);
  free(currentInode);
  free(indirectBlockIndex);
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  struct inode *node = get_inode(inum);
  if (node == NULL)
  {
    a.type = 0;
    a.atime = 0;
    a.mtime = 0;
    a.ctime = 0;
    a.size = 0;
  }
  else
  {
    a.type = (uint32_t) node->type;
    a.atime = node->atime;
    a.mtime = node->mtime;
    a.ctime = node->ctime;
    a.size = node->size;
    free(node);
  }
}


//part1c

void
inode_manager::remove_file(uint32_t inum)
{
  struct inode* currentInode = get_inode(inum);
  uint32_t blockCount = ((currentInode -> size - 1 + BLOCK_SIZE) / BLOCK_SIZE);
  if(blockCount <= NDIRECT)
  {
    for(uint32_t i = 0; i < blockCount; ++i)
    {
      bm -> free_block(currentInode -> blocks[i]);
    }
  }
  else
  {
    blockid_t *indirectBlockIndex = (blockid_t*)malloc(BLOCK_SIZE);
    bm->read_block(currentInode -> blocks[NDIRECT], (char*)indirectBlockIndex);
    for(uint32_t i = 0; i <= NDIRECT; ++i)
    {
      bm -> free_block(currentInode -> blocks[i]);
    }
    for(uint32_t i = 0; i < blockCount - NDIRECT; ++i)
    {
      bm -> free_block(indirectBlockIndex[i]);
    }
  }
  memset(currentInode, 0 , sizeof(struct inode));
  put_inode(inum, currentInode);
  free(currentInode);
}
