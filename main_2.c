
#include <stdint.h>
#include <stdio.h>


#define INT int32_t
#define UINT uint32_t
#define INT8 uint8_t
#define UINT64 uint64_t

#define Tuple INT*

// returned data type by relational operators
typedef struct _Table{
    UINT nattrs;
    UINT ntuples;
    Tuple tuples[];
} _Table;

int main(void) {
    
}