#include <stdio.h>
#include <stdlib.h>
#include "ro.h"
#include "db.h"
#include <unistd.h>
#include <fcntl.h>

typedef struct Environ Environ;
void init_environ(Environ *environ, Database *db, Conf *conf);
void free_environ(Environ *environ);
Environ global_environ;

void init(){
    // do some initialization here.

    // example to get the Conf pointer
    // Conf* cf = get_conf();

    // example to get the Database pointer
    // Database* db = get_db();
    init_environ(&global_environ, get_db(), get_conf());
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

typedef struct Page {
    UINT64 page_id;
    INT data[];
} Page;

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
    UINT oid;  // assum if oid is 0, then this slot is free
    INT prev_lru;
    // when it is in used, it is the vfd that is less recently used // -1, if this is the least recently used
    INT next_lru; // the vfd that is more recently used // -1, if this is the most recently used
} Vfd;

typedef struct FdBuffer {
    INT buffer_size;
    INT free_list_head; // -1, when no more free element
    INT lru_head;
    INT lru_tail;
    Vfd *vfds;
} FdBuffer;

void init_fd_buffer(FdBuffer *fd_buffer, Conf *conf) {
    fd_buffer->buffer_size = conf->file_limit;
    fd_buffer->free_list_head = 0;
    fd_buffer->lru_head = -1;
    fd_buffer->lru_tail = -1;
    fd_buffer->vfds = malloc(conf->file_limit * sizeof(Vfd));
}

void free_fd_buffer(FdBuffer *fd_buffer) {
    free(fd_buffer->vfds);
    free(fd_buffer);
}

// return the vfd
INT oid_access(FdBuffer *fd_buffer, UINT oid, Database * db) {
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

INT read_page(UINT oid, UINT offset, FdBuffer *fd_buffer, Database *db,
        Conf *conf, Page *ret_buffer) {
    INT vfd = oid_access(fd_buffer, oid, db);
    return internal_read_page(fd_buffer->vfds[vfd].fd, offset, conf->page_size, ret_buffer);
}

/* buffer pool helper functions */
typedef struct BufferTag {
    UINT oid;
    UINT64 page_id;
} BufferTag;

typedef struct BufferDesc
{
    BufferTag   page_id;     // ID of page contained in buffer
    // UINT64      buf_id;  // buffer's index number (from 0)
    // INT8        dirty_bit;
    UINT        pin_count;
    UINT        usage_count;
    UINT64      freeNext;  // link in freelist chain 
} BufferDesc;

typedef struct BufferPool {
    BufferDesc *directory;
    Page *pages;
    UINT next_victim;
    UINT free_head;
} BufferPool;

void init_buffer_directory_free_list(BufferDesc *directory, UINT buf_slots) {
    for (int i = 0; i < buf_slots; i++) {
        directory[i].freeNext = i + 1;
    }
}

void init_buffer_pool(BufferPool *buffer_pool, Conf *conf) {
    buffer_pool->directory = calloc(conf->buf_slots, sizeof(BufferDesc));
    init_buffer_directory_free_list(buffer_pool->directory, conf->buf_slots);
    buffer_pool->free_head = 0;
    buffer_pool->pages = malloc(conf->buf_slots * conf->page_size);
    buffer_pool->next_victim = 0;
}

void free_buffer_pool(BufferPool *buffer_pool) {
    free(buffer_pool->directory);
    free(buffer_pool->pages);
    free(buffer_pool);
}

INT8 eq_BufferTag(BufferTag pid1, BufferTag pid2) {
    return (pid1.oid == pid2.oid) && (pid1.page_id == pid2.page_id);
}

INT8 buffer_pool_find_index(BufferTag pid, BufferPool *buffer_pool,
        UINT buf_slots, UINT *found_index) {
    for (int i = 0; i < buf_slots; i++) {
        if (eq_BufferTag(buffer_pool->directory[i].page_id, pid)) {
            *found_index = i;
            return 1;
        }
    }
    return 0;
}

INT8 buffer_pool_has_free(BufferPool *buffer_pool, UINT buf_slots) {
    return buffer_pool->free_head == buf_slots;
}

UINT next_victim(UINT victim, UINT max) {
    UINT next = victim + 1;
    if (next == max) {
        next = 0;
    }
    return next;
}

INT8 get_victim_buffer(BufferPool *buffer_pool, UINT buf_slots,
        UINT *buf_index) {
    INT8 has_not_in_use = 0;
    for (int i = 0; i < buf_slots; i++) {
        if (buffer_pool->directory[i].pin_count == 1) {
            has_not_in_use = 1;
            break;
        }
    }
    if (!has_not_in_use) {
        return 0;
    }

    UINT victim = buffer_pool->next_victim;
    for (;1; victim = next_victim(victim, buf_slots)) {
        if (buffer_pool->directory[victim].pin_count == 0 &&
            buffer_pool->directory[victim].usage_count == 0) {
                *buf_index = victim;
                buffer_pool->next_victim = next_victim(victim, buf_slots);
                break;
        } else {
            if (buffer_pool->directory[victim].usage_count > 0) {
                buffer_pool->directory[victim].usage_count--;
            }
        }
    }
    return 1;
}
struct Environ {
    Database *db;
    Conf *conf;
    BufferPool *buffer_pool;
    FdBuffer *fd_buffer;
};

void init_environ(Environ *environ, Database *db, Conf *conf) {
    environ->db = db;
    environ->conf = conf;
    environ->buffer_pool = malloc(sizeof (BufferPool));
    init_buffer_pool(environ->buffer_pool, conf);
    environ->fd_buffer = malloc(sizeof(FdBuffer));
    init_fd_buffer(environ->fd_buffer, conf);
}

void free_environ(Environ *environ) {
    // do not need to free environ->db and eniron->conf as they will be freed 
    // afterwards
    free_buffer_pool(environ->buffer_pool);
    free_fd_buffer(environ->fd_buffer);
    free(environ);
}

INT8 request_page(BufferTag pid, Environ *environ, UINT *buf_index) {
    BufferPool *buffer_pool = environ->buffer_pool;
    UINT buf_slots = environ->conf->buf_slots;
    INT8 found = buffer_pool_find_index(pid, buffer_pool, buf_slots, 
        buf_index);
    if (!found) {
        if (buffer_pool_has_free(buffer_pool, buf_slots)) {
            // replace page
            INT8 success = get_victim_buffer(buffer_pool, buf_slots, buf_index);
            if (!success) return success;
            buffer_pool->directory[*buf_index].page_id = pid;
            read_page(pid.oid, pid.page_id, environ->fd_buffer, environ->db,
                environ->conf, &(environ->buffer_pool->pages[*buf_index]) );
        } else {
            *buf_index = buffer_pool->free_head;
            // update free head
            buffer_pool->free_head = 
                buffer_pool->directory[*buf_index].freeNext;
            // reset meta data for this buffer
            buffer_pool->directory[*buf_index].page_id = pid;
            buffer_pool->directory[*buf_index].pin_count = 0;
            buffer_pool->directory[*buf_index].usage_count = 0;
            buffer_pool->directory[*buf_index].freeNext = buf_slots;
            read_page(pid.oid, pid.page_id, environ->fd_buffer, environ->db,
                environ->conf, &(environ->buffer_pool->pages[*buf_index]) );
        }
    }
    buffer_pool->directory[*buf_index].pin_count += 1;
    return 1;
}

void release_page(BufferTag pid, BufferPool *buffer_pool, Conf *conf) {
    UINT buf_index;
    if (buffer_pool_find_index(pid, buffer_pool, conf, &buf_index)) {
        buffer_pool->directory[buf_index].pin_count -= 1;
    }
}
