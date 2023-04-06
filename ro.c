#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include "ro.h"
#include "db.h"
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

typedef struct Vfd {
    INT fd;
    UINT oid;  // assum if oid is 0, then this slot is free
    INT prev_lru;
    // when it is in used, it is the vfd that is less recently used // -1, if this is the least recently used
    INT next_lru; // the vfd that is more recently used // -1, if this is the most recently used
} Vfd;

typedef struct FdBuffer {
    UINT buffer_size;
    INT free_list_head; // -1, when no more free element
    INT lru_head;
    INT lru_tail;
    Vfd *vfds;
} FdBuffer;

typedef struct BufferTag {
    UINT oid;
    UINT64 page_id;
} BufferTag;

typedef struct Page {
    UINT64 page_id;
    INT data[];
} Page;

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


typedef struct Environ {
    Database *db;
    Conf *conf;
    BufferPool *buffer_pool;
    FdBuffer *fd_buffer;
} Environ;

void init_environ(Environ *environ, Database *db, Conf *conf);
void free_environ(Environ *environ);
Environ global_environ;
Page *request_page(BufferTag pid, Environ *environ);
void release_page(BufferTag pid, Environ *Environ);

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
    free_environ(&global_environ);
    printf("release() is invoked.\n");
}


void setBufferTag(BufferTag *buf_tag, UINT oid, UINT64 page_id) {
    buf_tag->oid = oid;
    buf_tag->page_id = page_id;
}

UINT64 max_tups_per_page(Conf *conf, UINT nattrs) {
    return (conf->page_size - sizeof(UINT64)) / nattrs;
}

UINT64 table_get_npages(Table *table, UINT page_size) {
    UINT64 actual_mem_per_page = page_size - sizeof(UINT64);
    // rounding up reference: https://stackoverflow.com/a/2422722
    return (table->ntuples * table->nattrs + (actual_mem_per_page - 1)) /
        actual_mem_per_page;
}

// tuple iterator
typedef struct BufferedScan {
    Page **buffered_pages;
    UINT buffered_pages_size;
    UINT curr_buffered_page_index;
    UINT64 curr_page;
    Tuple tup; // the next tuple
    UINT64 tup_id; // the index of the current tuple
    UINT64 npages; // number of pages in the table
    UINT64 ntuples; // number of tuples in the table
} BufferedScan;

void BufferedScan_set_buffer(BufferedScan *s, Page **new_buffer_pages, UINT
    buffered_pages_size, UINT64 curr_page) {

    s->curr_buffered_page_index = 0;
    s->tup_id = 0;
    s->buffered_pages = new_buffer_pages;
    s->buffered_pages_size = buffered_pages_size;
    if (s->buffered_pages_size == 0) {
        printf("Buffer size cannot be 0");
        exit(1);
    }
    s->curr_page = curr_page;
    s->tup = s->buffered_pages[s->curr_buffered_page_index]->data;
}

void startBufferedScan(Table *table, Page **buffered_pages, UINT64 curr_page,
    UINT buffered_pages_size, BufferedScan *s) {
    
    if (global_environ.conf->page_size <= sizeof(UINT64)) {
        printf("There are no tuples in a page. page size is too small.");
        exit(1);
    }

    s->ntuples = table->ntuples;
    if (table->ntuples == 0) {
        s->tup = NULL;
    }
    s->npages = table_get_npages(table, global_environ.conf->page_size);

    BufferedScan_set_buffer(s, buffered_pages, buffered_pages_size, curr_page);
}



Tuple BufferedScan_get_next_tup(BufferedScan *s, UINT nattrs) {
    UINT page_size = global_environ.conf->page_size;
    Tuple t = s->tup;
    if (t == NULL) return NULL;
    // advance the iterator
    s->tup_id += 1;
    if (s->tup_id == max_tups_per_page(global_environ.conf, nattrs)) {
        s->tup_id = 0;
        s->curr_page += 1;
        s->curr_buffered_page_index += 1;
        if (s->curr_page >= s->npages) {
            s->tup = NULL;
        }
    } else if (s->tup_id + page_size * s->curr_page >= s->ntuples) {
        // last tuple has been returned already
        s->tup = NULL;
    } else {
        s->tup = &(s->buffered_pages[s->curr_buffered_page_index]->
            data[nattrs * s->tup_id]);
    }
    return t;
}

typedef struct Scan {
    BufferedScan buffered_scan;
    BufferTag buf_tag;
    Page *page;
} Scan;

// initialize a Scan structure
void startScan(Table *table, Scan *s) {
    setBufferTag(&(s->buf_tag), table->oid, 0);
    s->page = request_page(s->buf_tag, &global_environ);
    if (s->page == NULL) {
        printf("fail to request page!");
        exit(1);
    }
    startBufferedScan(table, &(s->page), 0, 1, &(s->buffered_scan));
}

Tuple get_next_tup(Scan *s, UINT nattrs) {
    Tuple curr_tup = BufferedScan_get_next_tup(&(s->buffered_scan), nattrs);
    UINT page_size = global_environ.conf->page_size;
    // advance the iterator
    if (curr_tup == NULL) {

        if (s->buffered_scan.tup_id + page_size * s->buffered_scan.curr_page >=
            s->buffered_scan.ntuples) {
                release_page(s->buf_tag, &global_environ);
                return NULL;
        } else {
            release_page(s->buf_tag, &global_environ);
            // request next page
            s->buf_tag.page_id += 1;
            if (s->buf_tag.page_id == s->buffered_scan.npages) {
                return NULL;
            }
            s->page = request_page(s->buf_tag, &global_environ);
            BufferedScan_set_buffer(&(s->buffered_scan), &(s->page), 1,
                s->buf_tag.page_id);
            return BufferedScan_get_next_tup(&(s->buffered_scan), nattrs);
        }
    } else {
        return curr_tup;
    }
    return curr_tup;
}

Table *get_table(const char *table_name, Database *db) {
    for (UINT i = 0; i < db->ntables; i++) {
        if (strcmp(table_name, db->tables[i].name) == 0) {
            return &(db->tables[i]);
        }
    }
    return NULL;
}

typedef struct ExtendableOutputTable {
    _Table *output_table;
    UINT64 capacity; // start with 1 page
} ExtendableOutputTable; // add one page each time

ExtendableOutputTable initExtOutputTable(Conf *conf, UINT nattrs) {
    ExtendableOutputTable ret;
    ret.capacity = 0;
    ret.output_table = malloc(sizeof(_Table));
    ret.output_table->nattrs = nattrs;
    ret.output_table->ntuples = 0;
    return ret;
}

void expandOutputTable(ExtendableOutputTable *output_table, Conf *conf) {
    output_table->capacity += conf->page_size;
    _Table *temp = realloc(output_table->output_table,
        sizeof(_Table) + output_table->capacity);
    if (temp != NULL) {
        perror("realloc");
        exit(1);
    }
    output_table->output_table = temp;
}

Tuple copy_tuple(Tuple tup, UINT nattrs) {
    Tuple ret = malloc(sizeof(INT) * nattrs);
    memcpy(ret, tup, sizeof(INT) * nattrs);
    return ret;
}

void appendOutputTable(ExtendableOutputTable *out, Tuple tup, Conf *conf) {
    if (out->capacity == out->output_table->ntuples) {
        expandOutputTable(out, conf);
    }
    out->output_table->tuples[out->output_table->ntuples] = tup;
    out->output_table->ntuples++;
}

_Table* sel(const UINT idx, const INT cond_val, const char* table_name){
    
    printf("sel() is invoked.\n");

    Table *table = get_table(table_name, global_environ.db);
    if (table == NULL) {
        printf("table %s does not exist.", table_name);
        return NULL;
    }

    if (table->ntuples == 0) {
        _Table *output = malloc(sizeof(_Table));
        output->nattrs = table->nattrs;
        output->ntuples = 0;
        return output;
    }

    Scan s;
    startScan(table, &s);

    ExtendableOutputTable out = initExtOutputTable(
        global_environ.conf, table->nattrs);

    for (
        Tuple tup = get_next_tup(&s, table->nattrs);
        tup != NULL;
        tup = get_next_tup(&s, table->nattrs)) {
        if (tup[idx] == cond_val) {
            // append to _Table *
            appendOutputTable(&out, copy_tuple(tup, table->nattrs),
                global_environ.conf);
        }
    }

    return out.output_table;

}

Tuple mergeTuple(Tuple tup1, UINT nattrs1, Tuple tup2, UINT nattrs2) {
    Tuple output_tup = malloc(sizeof(INT) * (nattrs1 + nattrs2));
    memcpy(output_tup, tup1, nattrs1 * sizeof(INT));
    memcpy(&(output_tup[nattrs1]), tup2, nattrs2 * sizeof(INT));
    return output_tup;
}

void nestedLoopJoin(Table *R, const UINT idxR, Table *S, const UINT idxS,
        ExtendableOutputTable *output, INT8 swap) {

    Conf *conf = global_environ.conf;
    UINT64 npagesR = table_get_npages(R, conf->page_size);
    UINT N = conf->buf_slots;

    UINT64 nreadPagesR = 0;
    UINT64 nstartPageR = 0;
    Page **R_pages = malloc(sizeof(Page *) * (N - 1));

    while (nreadPagesR < npagesR) {

        // read N-1 pages of R
        for (UINT i = 0; i < N - 1; i++) {
            BufferTag bufTagR;
            setBufferTag(&bufTagR, R->oid, nreadPagesR);
            R_pages[i] = request_page(bufTagR, &global_environ);
            nreadPagesR++;
            if (nreadPagesR == npagesR) break;
        }

        BufferedScan scanR;
        startBufferedScan(R, R_pages, nstartPageR, nreadPagesR - nstartPageR,
            &scanR);

        Scan scanS;
        startScan(S, &scanS);

        for (Tuple r_tup = BufferedScan_get_next_tup(&scanR, R->nattrs);
            r_tup != NULL;
            r_tup = BufferedScan_get_next_tup(&scanR, R->nattrs)) {

            for (Tuple s_tup = get_next_tup(&scanS, S->nattrs);
                s_tup != NULL;
                s_tup = get_next_tup(&scanS, S->nattrs)) {

                if (r_tup[idxR] == s_tup[idxS]) {
                    Tuple output_tup;
                    if (swap) {
                        output_tup = mergeTuple(s_tup, S->nattrs,
                            r_tup, R->nattrs);
                    } else {
                        output_tup = mergeTuple(r_tup, R->nattrs,
                            s_tup, S->nattrs);
                    }
                    appendOutputTable(output, output_tup, conf);
                }
            }
        }

        // release N-1 pages of R
        for (UINT i = 0; i < N - 1; i++) {
            BufferTag bufTagR;
            setBufferTag(&bufTagR, R->oid, nstartPageR);
            release_page(bufTagR, &global_environ);
            nstartPageR++;
            if (nstartPageR == nreadPagesR) break;
        }
    }

    free(R_pages);
}

void sortBlock() {

}

void mergeSorted() {

}

void mergeJoin() {

}

void sortMergeJoin(Table outerRel, Table innerRel, _Table *output) {

}

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name){

    printf("join() is invoked.\n");
    Table *table1 = get_table(table1_name, global_environ.db);
    if (table1 == NULL) {
        printf("table %s does not exist.", table1_name);
        return NULL;
    }
    Table *table2 = get_table(table2_name, global_environ.db);
    if (table2 == NULL) {
        printf("table %s does not exist.", table2_name);
        return NULL;
    }

    if (table1->ntuples == 0 || table2->ntuples == 0) {
        _Table *output = malloc(sizeof(_Table));
        output->nattrs = table1->nattrs + table2->nattrs;
        output->ntuples = 0;
        return output;
    }

    ExtendableOutputTable out = initExtOutputTable(
        global_environ.conf, table1->nattrs + table2->nattrs);

    UINT64 page_size = global_environ.conf->page_size;
    if (global_environ.conf->buf_slots <
            table_get_npages(table1, page_size) + 
            table_get_npages(table2, page_size) ) {
        // nested loop join
        // outer should be the smaller table
        if (table1->ntuples > table2->ntuples) {
            nestedLoopJoin(table2, idx2, table1, idx1, &out, 1);
        } else {
            nestedLoopJoin(table1, idx1, table2, idx2, &out, 0);
        }
    } else {
        // sort merge join
    }

    // write your code to join two tables
    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    return out.output_table;
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


INT internal_read_page(INT fd, UINT64 offset, UINT page_size, void *buffer) {
    lseek(fd, offset * page_size, SEEK_SET);
    INT nbytes = read(fd, buffer, page_size);
    if (nbytes == -1) {
        perror("Error while reading a page");
        exit(1);
    }
    return nbytes;
}

/* fd pool helper functions */


void init_fd_buffer(FdBuffer *fd_buffer, Conf *conf) {
    fd_buffer->buffer_size = conf->file_limit;
    fd_buffer->free_list_head = 0;
    fd_buffer->lru_head = -1;
    fd_buffer->lru_tail = -1;
    fd_buffer->vfds = malloc(conf->file_limit * sizeof(Vfd));
}

void free_fd_buffer(FdBuffer *fd_buffer) {
    for (UINT i = 0; i < fd_buffer->buffer_size; i++) {
        if (fd_buffer->vfds[i].oid == 0) {
            close(fd_buffer->vfds[i].fd);
        }
    }
    free(fd_buffer->vfds);
    free(fd_buffer);
}

// return the vfd
INT oid_access(FdBuffer *fd_buffer, UINT oid, Database * db) {
    // search if oid is the vfd cache
    // NOTE: may use hash table to speed up the search
    for (UINT i = 0; i < fd_buffer->buffer_size; i++) {
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

void init_buffer_directory_free_list(BufferDesc *directory, UINT buf_slots) {
    for (UINT i = 0; i < buf_slots; i++) {
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
    for (UINT i = 0; i < buf_slots; i++) {
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
    for (UINT i = 0; i < buf_slots; i++) {
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
            log_release_page(buffer_pool->directory[victim].page_id.
                page_id);
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
}

Page *request_page(BufferTag pid, Environ *environ) {
    UINT buf_index;
    BufferPool *buffer_pool = environ->buffer_pool;
    UINT buf_slots = environ->conf->buf_slots;
    INT8 found = buffer_pool_find_index(pid, buffer_pool, buf_slots, 
        &buf_index);
    if (!found) {
        if (buffer_pool_has_free(buffer_pool, buf_slots)) {
            // replace page
            INT8 success = get_victim_buffer(buffer_pool, buf_slots, 
                &buf_index);
            if (!success) return NULL;
            buffer_pool->directory[buf_index].page_id = pid;
            log_read_page(pid.page_id);
            read_page(pid.oid, pid.page_id, environ->fd_buffer, environ->db,
                environ->conf, &(environ->buffer_pool->pages[buf_index]) );
        } else {
            buf_index = buffer_pool->free_head;
            // update free head
            buffer_pool->free_head = 
                buffer_pool->directory[buf_index].freeNext;
            // reset meta data for this buffer
            buffer_pool->directory[buf_index].page_id = pid;
            buffer_pool->directory[buf_index].pin_count = 0;
            buffer_pool->directory[buf_index].usage_count = 0;
            buffer_pool->directory[buf_index].freeNext = buf_slots;
            log_read_page(pid.page_id);
            read_page(pid.oid, pid.page_id, environ->fd_buffer, environ->db,
                environ->conf, &(buffer_pool->pages[buf_index]) );
        }
    }
    buffer_pool->directory[buf_index].pin_count += 1;
    buffer_pool->directory[buf_index].usage_count += 1;
    return &(buffer_pool->pages[buf_index]);
}

void release_page(BufferTag pid, Environ *environ) {
    UINT buf_index;
    BufferPool *buffer_pool = environ->buffer_pool;
    if (buffer_pool_find_index(pid, buffer_pool, environ->conf->buf_slots, &buf_index)) {
        buffer_pool->directory[buf_index].pin_count -= 1;
    }
}
