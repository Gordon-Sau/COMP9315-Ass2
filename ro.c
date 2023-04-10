#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include "ro.h"
#include "db.h"
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#define DEBUG 0

typedef INT8 bool;

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
    UINT64 page_index;
} BufferTag;

typedef struct Page {
    UINT64 page_id;
    INT data[];
} Page;

typedef struct BufferDesc {
    BufferTag   buf_tag;     // ID of page contained in buffer
    // UINT64      buf_id;  // buffer's index number (from 0)
    // INT8        dirty_bit;
    UINT        pin_count;
    UINT        usage_count;
} BufferDesc;

typedef struct BufferPool {
    BufferDesc *directory;
    INT8 *pages;
    UINT next_victim;
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
Page *request_page(BufferTag bufTag, Environ *environ);
void release_page(BufferTag bufTag, Environ *Environ);

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


void setBufferTag(BufferTag *buf_tag, UINT oid, UINT64 page_index) {
    buf_tag->oid = oid;
    buf_tag->page_index = page_index;
}

UINT64 max_tups_per_page(Conf *conf, UINT nattrs) {
    return ((conf->page_size - sizeof(UINT64)) / sizeof(INT)) / nattrs;
}

UINT64 round_up_div(UINT64 x, UINT64 y) {
    // rounding up reference: https://stackoverflow.com/a/2422722
    return (x + (y-1)) / y;
}

UINT64 table_get_npages(Table *table, UINT page_size) {
    UINT64 tups_per_page = max_tups_per_page(
        global_environ.conf, table->nattrs);
    return round_up_div(table->ntuples, tups_per_page);
}

// tuple iterator
typedef struct BufferedScan {
    Page **buffered_pages; // the buffer
    UINT n_buffered_pages; // number of buffered pages
    UINT curr_buffered_page_index; // the index in the buffer
    UINT64 curr_page; // the actual page index
    UINT64 tup_id; // the index of the current tuple
    UINT64 npages; // number of pages in the table
    UINT64 ntuples; // number of tuples in the table
    UINT nattrs; // number of attributes for each tuple in the table
} BufferedScan;

bool is_end_tup(BufferedScan *s) {
    UINT64 tups_per_page = max_tups_per_page(global_environ.conf, s->nattrs);
    if (s->curr_page * tups_per_page > s->ntuples) {
        return 1;
    }
    return (s->tup_id >= s->ntuples - s->curr_page * tups_per_page);
}

#if DEBUG
void print_bufferedScan_state(BufferedScan *s) {
    printf("BuffererdScan: curr_page: %lu, curr_buffered_page_index: %u, tup_id: %lu, nattrs: %u, buffered_pages: %p, curr_buffered_page: %p\n", s->curr_page, s->curr_buffered_page_index, s->tup_id, s->nattrs, s->buffered_pages, s->buffered_pages[s->curr_buffered_page_index]);
}
#endif

Tuple BufferedScan_get_tup_pointer(BufferedScan *s) {
    UINT64 tups_per_page = max_tups_per_page(global_environ.conf, s->nattrs);

    if (s->tup_id >= tups_per_page) {
        s->tup_id = 0;
        s->curr_page += 1;
        s->curr_buffered_page_index += 1;

        if (s->curr_page >= s->npages) {
            return NULL;
        }

        if (s->curr_buffered_page_index >= s->n_buffered_pages) {
            return NULL;
        }
    }

    if (is_end_tup(s)) {
        return NULL;
    }
#if DEBUG
    print_bufferedScan_state(s);
#endif
    return &(s->buffered_pages[s->curr_buffered_page_index]->
        data[s->tup_id * s->nattrs]);
}

void BufferedScan_set_buffer(BufferedScan *s, Page **new_buffer_pages, UINT
    n_buffered_pages, UINT64 curr_page) {

    s->curr_buffered_page_index = 0;
    s->tup_id = 0;
    s->buffered_pages = new_buffer_pages;
    s->n_buffered_pages = n_buffered_pages;
    if (s->n_buffered_pages == 0) {
        printf("Buffer size cannot be 0");
        exit(1);
    }
    s->curr_page = curr_page;
}

void startBufferedScan(Table *table, Page **buffered_pages, UINT64 curr_page,
    UINT n_buffered_pages, BufferedScan *s) {
    
    if (global_environ.conf->page_size <= sizeof(UINT64)) {
        printf("There are no tuples in a page. page size is too small.");
        exit(1);
    }

    s->ntuples = table->ntuples;
    s->nattrs = table->nattrs;
    s->npages = table_get_npages(table, global_environ.conf->page_size);

    BufferedScan_set_buffer(s, buffered_pages, n_buffered_pages, curr_page);
}

Tuple BufferedScan_get_next_tup(BufferedScan *s) {
    Tuple t = BufferedScan_get_tup_pointer(s);
    // end of table, do not advance the pointer
    if (t == NULL) return NULL;
    // advance the iterator
    s->tup_id += 1;
    return t;
}

typedef struct Scan {
    BufferedScan buffered_scan;
    BufferTag buf_tag; // current buffer tag
    Page *page; // current buffered page
} Scan;

// initialize a Scan structure
void startScan(Table *table, Scan *s) {
    setBufferTag(&(s->buf_tag), table->oid, 0);
    s->page = request_page(s->buf_tag, &global_environ);
    if (s->page == NULL) {
        printf("fail to request page!");
        exit(1);
    }
    startBufferedScan(table, &(s->page), s->buf_tag.page_index, 1,
        &(s->buffered_scan));
}

Tuple get_next_tup(Scan *s) {
    Tuple curr_tup = BufferedScan_get_next_tup(&(s->buffered_scan));
    // advance the iterator
    if (curr_tup == NULL) {
        release_page(s->buf_tag, &global_environ);
        if (is_end_tup(&(s->buffered_scan))) {
            return NULL;
        } else {
            // request next page
            s->buf_tag.page_index += 1;
            if (s->buf_tag.page_index >= s->buffered_scan.npages) {
                return NULL;
            }
            s->page = request_page(s->buf_tag, &global_environ);
            BufferedScan_set_buffer(&(s->buffered_scan), &(s->page), 1,
                s->buf_tag.page_index);
            return BufferedScan_get_next_tup(&(s->buffered_scan));
        }
    } else {
        return curr_tup;
    }
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
    UINT inc_cap = conf->page_size / sizeof(Tuple);
    if (inc_cap == 0) {
        inc_cap = 1;
    }
    output_table->capacity += inc_cap;
    _Table *temp = realloc(output_table->output_table,
        sizeof(_Table) + output_table->capacity * sizeof(Tuple));
    if (temp == NULL) {
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

#if DEBUG
void print_tup(Tuple tup, UINT nattrs) {
    if (tup != NULL) {
        for (UINT i = 0; i < nattrs; i++) {
            printf("%d ", tup[i]);
        }
        printf("\n");
    } else {
        printf("NULL\n");
    }

}

void print_table(const char *table_name) {
    Table *table = get_table(table_name, global_environ.db);
    Scan s;
    startScan(table, &s);
    for (
        Tuple tup = get_next_tup(&s);
        tup != NULL;
        tup = get_next_tup(&s)) {
        print_tup(tup, table->nattrs);
    }
}
#endif

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
        Tuple tup = get_next_tup(&s);
        tup != NULL;
        tup = get_next_tup(&s)) {
        print_tup(tup, table->nattrs);
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
    if (N == 1) {
        printf("cannot do nested loop join with just 1 buffer");
        exit(1);
    }

    UINT64 nreadPagesR = 0;
    UINT64 nstartPagesR = 0;
    Page **R_pages = malloc(sizeof(Page *) * (N - 1));

    while (nstartPagesR < npagesR) {

        // read N-1 pages of R
        for (UINT i = 0; i < N - 1; i++) {
            BufferTag bufTagR;
            setBufferTag(&bufTagR, R->oid, nreadPagesR);
            R_pages[i] = request_page(bufTagR, &global_environ);
            if (R_pages[i] == NULL) {
                printf("failed to request page in nested loop join! Accessing oid: %u, page_index: %lu",
                    bufTagR.oid, bufTagR.page_index);
            }
            nreadPagesR++;
            if (nreadPagesR == npagesR) break;
        }

        BufferedScan scanR;
        startBufferedScan(R, R_pages, nstartPagesR, nreadPagesR - nstartPagesR,
            &scanR);

        Scan scanS;
        startScan(S, &scanS);

        for (Tuple r_tup = BufferedScan_get_next_tup(&scanR);
            r_tup != NULL;
            r_tup = BufferedScan_get_next_tup(&scanR)) {

            for (Tuple s_tup = get_next_tup(&scanS);
                s_tup != NULL;
                s_tup = get_next_tup(&scanS)) {

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
            setBufferTag(&bufTagR, R->oid, nstartPagesR);
            release_page(bufTagR, &global_environ);
            nstartPagesR++;
            if (nstartPagesR == nreadPagesR) break;
        }
    }

    free(R_pages);
}

typedef struct SortedScan {
    UINT64 ntuple;
    UINT nattrs;
    INT *tuples;
    UINT64 curr_tup;
} SortedScan;

static int cmp_tuple(const void *p1, const void *p2, void *args) {
    const Tuple t1 = p1;
    const Tuple t2 = p2;
    UINT idx = *((UINT *)args);
    if (t1[idx] < t2[idx]) {
        return -1;
    } else if (t1[idx] > t2[idx]) {
        return 1;
    }
    return 0;
}

void sort_table(Table *table, UINT idx, SortedScan *s) {
    s->ntuple = table->ntuples;
    s->nattrs = table->nattrs;
    s->curr_tup = 0;

    // get all tuples and copy them to a new buffer
    s->tuples = malloc(sizeof(INT) * table->nattrs * table->ntuples);
    Scan input_scan;
    startScan(table, &input_scan);
    UINT64 curr_tup_id = 0;
    for (Tuple tup = get_next_tup(&input_scan);
        tup != NULL;
        tup = get_next_tup(&input_scan)) {

        memcpy(&(s->tuples[curr_tup_id * table->nattrs]), tup,
            table->nattrs * sizeof(INT));
        curr_tup_id++;
    }

    // sort the buffer
    qsort_r(s->tuples, table->ntuples, sizeof(INT) * s->nattrs, cmp_tuple, &idx);
}

Tuple SortedScan_get_next(SortedScan *s) {
    if (s->curr_tup >= s->ntuple) {
        return NULL;
    }
    printf("curr_tup: %lu\n", s->curr_tup);
    Tuple tup = &(s->tuples[s->curr_tup * s->nattrs]);
    s->curr_tup++;
    return tup;
}

UINT64 SortedScan_get_curr_tup_id(SortedScan *s) {
    return s->curr_tup;
}

void SortedScan_set_tup_id(SortedScan *s, UINT64 tup_id) {
    s->curr_tup = tup_id;
}

void merge_join(SortedScan *sortedR, SortedScan *sortedS, UINT idxR, UINT idxS,
    ExtendableOutputTable *output) {
#if DEBUG
    printf("merging:\n");
#endif
    Tuple tupR = SortedScan_get_next(sortedR);
    Tuple tupS;
    while (tupR != NULL &&
        (tupS = SortedScan_get_next(sortedS)) != NULL) {

        #if DEBUG
                printf("A\n");
                print_tup(tupR, sortedR->nattrs);
                print_tup(tupS, sortedS->nattrs);
        #endif

        while (tupR != NULL && (tupR[idxR] < tupS[idxS])) {
            tupR = SortedScan_get_next(sortedR);
            #if DEBUG
                    printf("B\n");
                    print_tup(tupR, sortedR->nattrs);
                    print_tup(tupS, sortedS->nattrs);
            #endif
        }
        if (tupR == NULL) break;

        while (tupS != NULL && (tupS[idxS] < tupR[idxR])) {
            tupS = SortedScan_get_next(sortedS);
            #if DEBUG
                    printf("C\n");
                    print_tup(tupR, sortedR->nattrs);
                    print_tup(tupS, sortedS->nattrs);
            #endif
        }
        if (tupS == NULL) break;

        // found equal
        // store the tuple id of the start of the current run
        UINT64 start_run_id = SortedScan_get_curr_tup_id(sortedS) - 1;
#if DEBUG
        printf("start_run_id: %lu\n", start_run_id);
#endif
        while (tupR != NULL && (tupR[idxR] == tupS[idxS])) {
            #if DEBUG
                    printf("D\n");
                    print_tup(tupR, sortedR->nattrs);
                    print_tup(tupS, sortedS->nattrs);
            #endif
            while (tupS != NULL && (tupR[idxR] == tupS[idxS])) {
                appendOutputTable(
                    output,
                    mergeTuple(tupR, sortedR->nattrs,tupS,sortedS->nattrs),
                    global_environ.conf);
                tupS = SortedScan_get_next(sortedS);
            }
            tupR = SortedScan_get_next(sortedR);
            SortedScan_set_tup_id(sortedS, start_run_id);
            #if DEBUG
                    printf("E\n");
                    print_tup(tupR, sortedR->nattrs);
                    print_tup(tupS, sortedS->nattrs);
            #endif
        }

    }
}

void freeSortedScan(SortedScan *s) {
    free(s->tuples);
}

#if DEBUG
void print_sorted_table(SortedScan *s) {
    Tuple tup = SortedScan_get_next(s);
    while (tup != NULL) {
        print_tup(tup, s->nattrs);
        tup = SortedScan_get_next(s);
    }
    printf("\n");
    SortedScan_set_tup_id(s, 0);
}
#endif

void sortMergeJoin(Table *R, UINT idxR, Table *S, UINT idxS,
        ExtendableOutputTable *output) {
    SortedScan sortedR;
    sort_table(R, idxR, &sortedR);
#if DEBUG
    printf("sortedScan: \n");
    print_sorted_table(&sortedR);
#endif
    SortedScan sortedS;
    sort_table(S, idxS, &sortedS);
#if DEBUG
    print_sorted_table(&sortedS);
#endif
    merge_join(&sortedR, &sortedS, idxR, idxS, output);
    freeSortedScan(&sortedR);
    freeSortedScan(&sortedS);
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
#if DEBUG
        printf("nested loop join invoke()\n");
#endif
        // outer should be the smaller table
        if (table1->ntuples > table2->ntuples) {
            nestedLoopJoin(table2, idx2, table1, idx1, &out, 1);
        } else {
            nestedLoopJoin(table1, idx1, table2, idx2, &out, 0);
        }
    } else {
        // sort merge join
#if DEBUG
        printf("sort merge join invoke()\n");
#endif
        sortMergeJoin(table1, idx1, table2, idx2, &out);
    }

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
        if (fd_buffer->vfds[i].oid != 0) {
            internal_close_file(fd_buffer->vfds[i].oid, fd_buffer->vfds[i].fd);
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

void init_buffer_pool(BufferPool *buffer_pool, Conf *conf) {
    buffer_pool->directory = calloc(conf->buf_slots, sizeof(BufferDesc));
    buffer_pool->pages = malloc(conf->buf_slots * conf->page_size);
#if DEBUG
    memset(buffer_pool->pages, 0, conf->buf_slots * conf->page_size);
#endif
    buffer_pool->next_victim = 0;
}

void free_buffer_pool(BufferPool *buffer_pool) {
    free(buffer_pool->directory);
    free(buffer_pool->pages);
    free(buffer_pool);
}

bool eq_BufferTag(BufferTag bufTag1, BufferTag bufTag2) {
    return (bufTag1.oid == bufTag2.oid) &&
        (bufTag1.page_index == bufTag2.page_index);
}

bool buffer_pool_find_index(BufferTag bufTag, BufferPool *buffer_pool,
        UINT buf_slots, UINT *found_index) {
    for (UINT i = 0; i < buf_slots; i++) {
        if (eq_BufferTag(buffer_pool->directory[i].buf_tag, bufTag)) {
            *found_index = i;
            return 1;
        }
    }
    return 0;
}

UINT next_victim(UINT victim, UINT buf_size) {
    UINT next = victim + 1;
    if (next == buf_size) {
        next = 0;
    }
    return next;
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

Page *buffer_get_page_ptr(const BufferPool *buffer_pool, const UINT buf_index,
const UINT page_size) {
    return ((Page *)(&(buffer_pool->pages[buf_index * page_size])));
}

Page *request_page(BufferTag bufTag, Environ *environ) {
    UINT buf_index;
    BufferPool *buffer_pool = environ->buffer_pool;
    UINT buf_slots = environ->conf->buf_slots;
    INT8 found = buffer_pool_find_index(bufTag, buffer_pool, buf_slots, 
        &buf_index);
    UINT page_size = environ->conf->page_size;
    if (!found) {
        // for debugging
        {
            bool has_free = 0;
            for (int i = 0; i < buf_slots; i++) {
                if (buffer_pool->directory[i].pin_count == 0) {
                    has_free = 1;
                    break;
                }
            }
            if (!has_free) {
                printf("ERROR: no free page to be eviceted! acessing %u %lu",
                    bufTag.oid, bufTag.page_index);
                return NULL;
            }
        }

        INT victim = buffer_pool->next_victim;
        for (;1; victim = next_victim(victim, buf_slots)) {
            if (buffer_pool->directory[victim].pin_count == 0 &&
                    buffer_pool->directory[victim].usage_count == 0) {
                buf_index = victim;
                Page *page_ptr = buffer_get_page_ptr(buffer_pool, buf_index, page_size);
                if (buffer_pool->directory[buf_index].buf_tag.oid != 0) {
                    log_release_page(page_ptr->page_id);
                } 
                read_page(bufTag.oid, bufTag.page_index, environ->fd_buffer,
                    environ->db, environ->conf, page_ptr);
                setBufferTag(&(buffer_pool->directory[buf_index].buf_tag),
                    bufTag.oid, bufTag.page_index);
                log_read_page(page_ptr->page_id);
                buffer_pool->next_victim = next_victim(victim, buf_slots);
                break;
            } else {
                if (buffer_pool->directory[victim].usage_count > 0) {
                    buffer_pool->directory[victim].usage_count--;
                }
            }
        }
    } else {
#if DEBUG
        printf("requested page oid: %u, page_index: %lu already exists in the buffer!\n", bufTag.oid, bufTag.page_index);
#endif
    }
    buffer_pool->directory[buf_index].pin_count = 1;
    buffer_pool->directory[buf_index].usage_count += 1;
#if DEBUG
    printf("page oid: %u, page_index: %lu is allocated at buf_index: %u (pointer: %p)\n", bufTag.oid, bufTag.page_index, buf_index, &(buffer_pool->pages[buf_index]));
    printf("buffer:\n");
    for (UINT i = 0; i < buf_slots; i++) {
        Page *page_ptr = buffer_get_page_ptr(buffer_pool, i, page_size);
        printf("\tpage_id: %lu\n\tdata: ", page_ptr->page_id);
        for (UINT j = 0; j < (page_size - sizeof(UINT64)) / sizeof(INT); j++) {
            printf("%d,", page_ptr->data[j]);
        }
        printf("\n");
    }
#endif
    return buffer_get_page_ptr(buffer_pool, buf_index, page_size);
}

void release_page(BufferTag bufTag, Environ *environ) {
    UINT buf_index;
    BufferPool *buffer_pool = environ->buffer_pool;
    if (buffer_pool_find_index(bufTag, buffer_pool, environ->conf->buf_slots, &buf_index)) {
        buffer_pool->directory[buf_index].pin_count = 0;
    }
}
