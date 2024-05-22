/*
MIT License

Copyright (c) 2018 bert hubert

Permission is hereby granted, free of charge, to any person obtaining a copy
*/
#ifndef LMDB_SAFE_GLOBAL
#define LMDB_SAFE_GLOBAL

#ifndef LMDB_SAFE_NO_CPP_UTILITIES
#include <c++utilities/application/global.h>
#else
#undef LMDB_SAFE_STATIC
#define LMDB_SAFE_STATIC 1
#endif

#ifdef LMDB_SAFE_STATIC
#define LMDB_SAFE_EXPORT
#define LMDB_SAFE_IMPORT
#else
#define LMDB_SAFE_EXPORT CPP_UTILITIES_GENERIC_LIB_EXPORT
#define LMDB_SAFE_IMPORT CPP_UTILITIES_GENERIC_LIB_IMPORT
#endif

/*!
 * \def LMDB_SAFE_EXPORT
 * \brief Marks the symbol to be exported by the lmdb-safe library.
 */

/*!
 * \def LMDB_SAFE_IMPORT
 * \brief Marks the symbol to be imported from the lmdb-safe library.
 */

#endif // LMDB_SAFE_GLOBAL
