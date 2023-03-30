#include <stdio.h>
#include <stdlib.h>
#include "ro.h"
#include "db.h"
#include <unistd.h>
#include <fcntl.h>

void init(){
    // do some initialization here.

    // example to get the Conf pointer
    // Conf* cf = get_conf();

    // example to get the Database pointer
    // Database* db = get_db();
    
    printf("init() is invoked.\n");
}

void release(){
    // optional
    // do some end tasks here.
    // free space to avoid memory leak
    printf("release() is invoked.\n");
}

_Table* sel(const UINT idx, const INT cond_val, const char* table_name){
    
    printf("sel() is invoked.\n");

    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    // testing
    // the following code constructs a synthetic _Table with 10 tuples and each tuple contains 4 attributes
    // examine log.txt to see the example outputs
    // replace all code with your implementation

    UINT ntuples = 10;
    UINT nattrs = 4;

    _Table* result = malloc(sizeof(_Table)+ntuples*sizeof(Tuple));
    result->nattrs = nattrs;
    result->ntuples = ntuples;

    INT value = 0;
    for (UINT i = 0; i < result->ntuples; i++){
        Tuple t = malloc(sizeof(INT)*result->nattrs);
        result->tuples[i] = t;
        for (UINT j = 0; j < result->nattrs; j++){
            t[j] = value;
            ++value;
        }
    }
    
    return result;

    // return NULL;
}

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name){

    printf("join() is invoked.\n");
    // write your code to join two tables
    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    return NULL;
}

INT internal_open_file(UINT oid, Database *db) {
    log_open_file(oid);

    char table_path[200];
    sprintf(table_path,"%s/%u",db->path,oid);
    INT fd = open(table_path, O_RDWR);

    if (fd == -1) {
        perror("Error while opening file");
        exit(1);
    }
    return fd;
}


void internal_close_file(UINT oid, INT fd) {
    log_close_file(oid);
    close(fd);
}


INT internal_read_page(INT fd, UINT offset, UINT page_size, void *buffer) {
    lseek(fd, offset * page_size, SEEK_SET);
    INT nbytes = read(fd, buffer, page_size);
    if (nbytes == -1) {
        perror("Error while reading a page");
        exit(1);
    }
    return nbytes;
}

/* fd pool helper functions */
typedef struct Vfd {
    INT fd;
    INT oid;  // assum if oid is 0, then this slot is free
    INT prev_lru;
    // when it is in used, it is the vfd that is less recently used // -1, if this is the least recently used
    INT next_lru; // the vfd that is more recently used // -1, if this is the most recently used
} Vfd;

typedef struct fd_buffer {
    INT buffer_size;
    INT free_list_head; // -1, when no more free element
    INT lru_head;
    INT lru_tail;
    Vfd *vfds;
} fd_buffer;

void init_fd_buffer(fd_buffer *fd_buffer, Conf *conf) {
    fd_buffer->buffer_size = conf->file_limit;
    fd_buffer->free_list_head = 0;
    fd_buffer->lru_head = -1;
    fd_buffer->lru_tail = -1;
    fd_buffer->vfds = malloc(conf->file_limit * sizeof(Vfd));
}

void free_fd_buffer(fd_buffer *fd_buffer) {
    free(fd_buffer->vfds);
    free(fd_buffer);
}

// return the vfd
INT oid_access(fd_buffer *fd_buffer, INT oid, Database * db) {
    // search if oid is the vfd cache
    // NOTE: may use hash table to speed up the search
    for (INT i = 0; i < fd_buffer->buffer_size; i++) {
        if (fd_buffer->vfds[i].oid == oid) {
            if (fd_buffer->lru_tail != i) {
                // not mru already
                // need to set it to the most recently used
                if (fd_buffer->lru_head == i) {
                    // lru
                    fd_buffer->lru_head = fd_buffer->vfds[i].next_lru;
                    fd_buffer->vfds[fd_buffer->lru_head].prev_lru = -1;
                } else {
                    // middle
                    INT prev = fd_buffer->vfds[i].prev_lru;
                    INT next = fd_buffer->vfds[i].next_lru;
                    fd_buffer->vfds[prev].next_lru = next;
                    fd_buffer->vfds[next].prev_lru = prev;
                }
                fd_buffer->vfds[fd_buffer->lru_tail].next_lru = i;
                fd_buffer->vfds[i].prev_lru = fd_buffer->lru_tail;
                fd_buffer->lru_tail = i;
                fd_buffer->vfds[i].next_lru = -1;
            }
            return i;
        }
    }

    // not existed already
    if (fd_buffer->free_list_head != -1) {
        // use the free slot
        INT vfd = fd_buffer->free_list_head;

        INT new_fd = internal_open_file(oid, db);
        fd_buffer->vfds[vfd].fd = new_fd;
        fd_buffer->vfds[vfd].oid = oid;
        fd_buffer->vfds[vfd].next_lru = -1;
        fd_buffer->vfds[vfd].prev_lru = fd_buffer->lru_tail;
        
        fd_buffer->lru_tail = vfd;
        if (fd_buffer->lru_head == -1) {
            fd_buffer->lru_head = vfd;
        }

        fd_buffer->free_list_head++;
        if (fd_buffer->free_list_head == fd_buffer->buffer_size) {
            fd_buffer->free_list_head = -1;
        }
        return vfd;
    } else {
        // no more free slot
        // need to close the least recently used file and reuse the slot
        INT vfd = fd_buffer->lru_head;
        if (vfd == -1) {
            puts("no free slots and no least recently used fd");
            exit(1);
        }
        internal_close_file(fd_buffer->vfds[vfd].oid, fd_buffer->vfds[vfd].fd);
        fd_buffer->vfds[vfd].oid = oid;
        fd_buffer->vfds[vfd].fd = internal_open_file(oid, db);

        if (fd_buffer->lru_tail == vfd) {
            // singleton
            // keep unchanged
        } else {
            fd_buffer->lru_head = fd_buffer->vfds[vfd].next_lru;
            // set mru
            fd_buffer->vfds[fd_buffer->lru_tail].next_lru = vfd;
            fd_buffer->vfds[vfd].prev_lru = fd_buffer->lru_tail;
            fd_buffer->vfds[vfd].next_lru = -1;
            fd_buffer->lru_tail = vfd;
        }
        return vfd;
    }

}

INT read_page(INT oid, UINT offset, fd_buffer *fd_buffer, Database *db, Conf *conf, void *ret_buffer) {
    INT vfd = oid_access(fd_buffer, oid, db);
    return internal_read_page(fd_buffer->vfds[vfd].fd, offset, conf->page_size, ret_buffer);
}

/* buffer pool helper functions */
typedef struct PageId {
    INT oid;
    INT page_id;
} PageId;

typedef struct BufferDesc
{
    PageId     page_id;     // ID of page contained in buffer
    INT        buf_id;  // buffer's index number (from 0)
    // state, containing flags, refcount and usagecount
    INT        state;
    INT        freeNext;  // link in freelist chain 
} BufferDesc;

typedef struct buffer_pool {

} buffer_pool;

void inin_buffer_pool(buffer_pool *buffer_pool) {

}


