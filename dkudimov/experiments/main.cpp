#include "int.h"

int main()
{
    TCleaner cleaner;
    TStateCache cache(10000, 3, cleaner);



    return 0;
}