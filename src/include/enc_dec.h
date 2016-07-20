// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Allen Samuels <allen.samuels@sandisk.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef _ENC_DEC_H
#define _ENC_DEC_H


#include <set>
#include <map>
#include <vector>
#include <string>
#include <string.h>
#include <type_traits>

/*******************************************************

  
   New fast encode/decode framework.
  
   The entire framework is built around the idea that each object has three operations:
  
     ESTIMATE  -- worst-case estimate of the amount of storage required for this object
     ENCODE    -- encode object into buffer of size ESTIMATE
     DECODE    -- encode object from buffer of size actual.
  
   Each object has a single templated function that actually provides all three operations in a single set of code.
   But doing this, it's pretty much guaranteed that the ESTIMATE and the ENCODE code are in harmony (i.e. that the estimate is correct)
   it also saves a lot of typing/reading...
  
   Generally, all three operations are provided on a single function name with the input and return parameters overloaded to distinguish them.
  
   It's observed that for each of the three operations there is a single value which needs to be transmitted between each of the micro-encode/decode calls
   Yes, this is confusing, but let's look at a simple example
  
    struct simple {
      int a;
      float b;
      std::string c;
      set<int> d;
    };
  
    To encode this struct we generate a function that does the micro-encoding of each of the fields of the struct
    Here's an example of a function that does the ESTIMATE operation.
  
    size_t simple::estimate() {
       return 
          sizeof(a) +
          sizeof(b) +
          c.size() +
          d.size() * sizeof(int);
    }

    We're going to re-write it as:

    size_t simple::estimate(size_t p) {
       p = estimate(p,a);
       p = estimate(p,b);
       p = estimate(p,c);
       p = estimate(p,d);
       return p;
    }

    assuming that the sorta function:

    template<typename t> size_t estimate(size_t p,t& o) { return p + sizeof(o); }
    template<typename t> size_t estimate(size_t p,set<t>& o) { return p + o.size() * sizeof(t); }
    

    similarly, the encode operation is represented as:

    char * simple::encode(char *p) {
       p = encode(p,a);
       p = encode(p,b);
       p = encode(p,c);
       p = encode(p,d);
       return p;
    }
       
    similarly, the decode operation is represented as:

    const char * simple::decode(const char *p) {
       p = decode(p,a);
       p = decode(p,b);
       p = decode(p,c);
       p = decode(p,d);
       return p;
    }
       

You can now see that it's possible to create a single function that does all three operations in a single block
of code, provided that you can fiddle the input/output parameter types appropriately.

In essence the pattern is

    p = enc_dec(p,struct_field_1);
    p = enc_dec(p,struct_field_2);
    p = enc_dec(p,struct_field_3);

With the type of p being set differently for each operation, i.e.,
    for ESTIMATE, p = size_t
    for ENCODE,   p = char *
    for DECODE,   p = const char *

This is the essence of how the encode/decode framework operates. Though there is some more sophistication...

----------------------

We also want to allow the encode/decode machinery to be per-type and to operate more or less automatically for us in many situations.

We create a struct called "enc_dec_traits". This struct contains per-type information that drives the estimation machinery...

*****************************************************************************/

//
// A key optimization for containers is whether the ESTIMATE operation is required to walk the container or not.
// For types like "int32" that serialize into a small number of bytes -- worst-case -- we're happy if the estimation operation for a 
// container just uses that max-size * container.size(), which is O(1) and avoids that walk. We use this member of this traits struct
// to control this machinery.
//
// Generally, it is assumed that all types are unbounded in size (i.e., not subject to the optimization)
// Currently, the primitive types (int, char, short, long long, etc.) are automatically enabled for this optimization by the infrastructure.
// When you declare a complex type serializer usage of the appropriate macro (see below) automatically sets this for you.
//
template<typename t> struct enc_dec_traits;

//
// Just like the existing encode/decode machinery. The environment provides a rich set of 
// pre-defined encodes for primitive types and containers
//
// These macros all handle byte-ordering automatically for you :) All external serializations are in little endian order
//
// itype == internal type
// otype == external type, i.e., the type on the wire
//

#define DEFINE_ENC_DEC_SIMPLE(itype,etype) \
template<> struct enc_dec_traits<itype> { enum { is_bounded_size = true }; }; \
inline size_t      enc_dec(size_t p,itype &o)      { return p + sizeof(etype); } \
inline char *      enc_dec(char *p, itype &o)      { *(etype *)p = static_cast<etype>(o); return p + sizeof(etype); } \
inline const char *enc_dec(const char *p,itype &o) { o = static_cast<itype>(*(const etype *)p); return p + sizeof(etype); }

typedef unsigned int uint32;

DEFINE_ENC_DEC_SIMPLE(char,char);
DEFINE_ENC_DEC_SIMPLE(unsigned char,char);
DEFINE_ENC_DEC_SIMPLE(short,short);		 // __le16
DEFINE_ENC_DEC_SIMPLE(unsigned short,short);     // __le16
DEFINE_ENC_DEC_SIMPLE(int,int);                  // __le32.
DEFINE_ENC_DEC_SIMPLE(unsigned int,int);	 // __le32
DEFINE_ENC_DEC_SIMPLE(size_t,uint32);	         // __le32, we never encode anything larger than 2^32 :)
DEFINE_ENC_DEC_SIMPLE(long long,long long);
DEFINE_ENC_DEC_SIMPLE(unsigned long long,long long);

//
// std::string encode/decode
//
template<> struct enc_dec_traits<std::string> { enum { is_bounded_size = false }; };
inline size_t enc_dec(size_t p,std::string& s) { return p + sizeof(size_t) + s.size(); }
inline char * enc_dec(char * p,std::string& s) { *(size_t *)p = s.size(); memcpy(p+sizeof(size_t),s.c_str(),s.size()); return p + sizeof(size_t) + s.size(); }
inline const char *enc_dec(const char *p,std::string& s) { s = std::string(p + sizeof(size_t),*(size_t *)p); return p + sizeof(size_t) + s.size(); }

//
// unsigned VarInt
//
// High bit of each byte is a "stop" bit. "1" indicates last byte. "0" indicates more data to come
//
#define ENABLE_IF_UNSIGNED(x,r) typename std::enable_if<not std::is_signed<x>::value,r>::type
template<typename t> ENABLE_IF_UNSIGNED(t,size_t)
inline enc_dec_varint(size_t p,t& o) { return p + sizeof(t) + 2; } // two extra bytes is maximum growth due to encoding

template<typename t> ENABLE_IF_UNSIGNED(t,char *)
inline enc_dec_varint(char * p,t& o) {
   t v = o;
   while (v >= 0x80) {
      *p++ = v & 0x7F;
      v >>= 7;
   }
   *p++ = v | 0x80;
   return p;
}

template<typename t> ENABLE_IF_UNSIGNED(t,const char *)
inline enc_dec_varint(const char *p,t& o) {
   t v = 0;
   int shift = 0;
   while (1) {
      char temp = *p++;
      v |= t(temp & 0x7F) << shift;
      if (temp & 0x80) break;
      shift += 7;
   }
   o = v;
   return p;
}

//
// signed VarInt
//
// High bit of each byte is a "stop" bit. "1" indicates last byte. "0" indicates more data to come
// Next to high bit of last byte is sign of result, if "1" stored value is 2's complement of the answer
//

#define ENABLE_IF_SIGNED(x,r) typename std::enable_if<std::is_signed<x>::value,r>::type
template<typename t> ENABLE_IF_SIGNED(t,size_t)
inline enc_dec_varint(size_t p,t& o) { return p + sizeof(t) + 2; } // two extra bytes is maximum growth due to encoding

template<typename t> ENABLE_IF_SIGNED(t,char *)
inline enc_dec_varint(char * p,t& o) {
   char flag;
   typedef typename std::make_unsigned<t>::type us_t;
   us_t v;
   if (o < 0) {
      flag = 0xC0;
      v = us_t(-o);
   } else {
      flag = 0x80;
      v = us_t(o);
   }
   while (v >= 0x40) {
      *p++ = v & 0x7F;
      v >>= 7;
   }
   *p++ = v | flag;
   return p;
}

template<typename t> ENABLE_IF_SIGNED(t,const char *)
inline enc_dec_varint(const char *p,t& o) {
   t v = 0;
   int shift = 0;
   while (1) {
      char temp = *p++;
      if (temp & 0x80) {
         v |= t(temp & 0x3F) << shift;
         if (temp & 0x40) {
	    o = -v;
         } else {
            o = v;
         }
         break;
      } else {
         v |= t(temp & 0x7F) << shift;
         shift += 7;
      }
   }
   return p;
}

//
// A pair of values.
//
template<typename f,typename s>
inline size_t enc_dec_pair(size_t p,f& first,s& second) {
   p = enc_dec(p,first);
   p = enc_dec(p,second);
   return p;
}

template<typename f,typename s>
inline char *enc_dec_pair(char *p,f& first,s& second) {
   p = enc_dec(p,first);
   p = enc_dec(p,second);
   return p;
}

template<typename f,typename s>
inline const char *enc_dec_pair(const char *p,f& first,s& second) {
   p = enc_dec(p,first);
   p = enc_dec(p,second);
   return p;
}

//
// Let's do a container.
//
// The trick with containers is to separate the functionality of serializing elements of the container with the serialization
// of those individual elements themselves. We do this by creating a template for the container and pass into it a function
// that serializes the individual elements, this defaults to the standard serialization function for that element type. However
// you CAN override that with a custom serialization function.
//
// As described earlier, we also have the optimization of whether the size estimation function needs to crawl the entire container
// accumulating the size of each element OR whether a worst-case estimate can be made by use the O(1) size() member of the container
// multiplied by the worst-case size of an encoded elements (which is determined by serializing a "ghost" element of that type, this
//    is done by calling the standard size estimation function but passing in a null pointer (as reference ;-) for the data. Meaning
//    that this call will fault out if the size estimation function actually tries to access the data (i.e., is data dependent in it's result)
//
//
// For example:
//
//  struct .... {
//      set<int> x;
//  };
//
//   in the serialization function we can do this:
//
//      p = enc_dec(p,x);  // use default encoding for <int> and use default is_bounded_size<int> [true].
//
//   An alternate would be:
//
//      p = enc_dec(p,x,enc_dec_varint); //use varint encoding and default is_bounded_size<int> [true].
//
//

//
// context.
//
// This functions as a "global" parameter to the set and multiset container encode operations
//
// If you want to have an encode of the key that is dependent on some external
// global information, create a derived type that overrides the three enc_dec functions as well as
// having whatever global information you want to include.
//
// Note: the global information in this context is NOT encoded into the buffer itself, that's your problem!
//
// Note: this object is passed by value into the enc_dec functions (i.e., copied). Plan accordingly.
//
template<typename k>
struct enc_dec_set_context {
   virtual size_t      operator()(size_t      p,k& key) { return enc_dec(p,key); }
   virtual char *      operator()(char       *p,k& key) { return enc_dec(p,key); }
   virtual const char *operator()(const char *p,k& key) { return enc_dec(p,key); }
   virtual ~enc_dec_set_context() {}
};

template<typename t> 
inline size_t enc_dec(
   size_t p,
   set<t>& s,
   enc_dec_set_context<t> c = enc_dec_set_context<t>(),
   bool is_bounded_size = enc_dec_traits<t>::is_bounded_size) {
   size_t sz;
   p = enc_dec(p,sz);
   if (is_bounded_size) {
      p += s.size() * c(size_t(0),*(t *) 0);
   } else {
      for (const t&e : s) {
         p = c(p,const_cast<t&>(e));
      }
   }
   return p;
}

template<typename t>
inline char *enc_dec(
   char *p,
   set<t>& s,
   enc_dec_set_context<t> c = enc_dec_set_context<t>()
   ) {
   size_t sz = s.size();
   p = enc_dec(p,sz);
   for (const t& e : s) {
      p = c(p,const_cast<t&>(e));
   }
   return p;
}

template<typename t>
inline const char *enc_dec(
   const char *p,
   set<t>&s,
   enc_dec_set_context<t> c = enc_dec_set_context<t>()
   ) {
   size_t sz;
   p = enc_dec(p,sz);
   while (sz--) {
      t temp;
      p = c(p,temp);
      s.insert(temp);
   }
   return p;
}

template<typename t> 
inline size_t enc_dec(
   size_t p,
   multiset<t>& s,
   enc_dec_set_context<t> c = enc_dec_set_context<t>(),
   bool is_bounded_size = enc_dec_traits<t>::is_bounded_size) {
   size_t sz;
   p = enc_dec(p,sz);
   if (is_bounded_size) {
      p += s.size() * c(size_t(0),*(t *) 0);
   } else {
      for (const t&e : s) {
         p = c(p,const_cast<t&>(e));
      }
   }
   return p;
}

template<typename t>
inline char *enc_dec(
   char *p,
   multiset<t>& s,
   enc_dec_set_context<t> c = enc_dec_set_context<t>()
   ) {
   size_t sz = s.size();
   p = enc_dec(p,sz);
   for (const t& e : s) {
      p = c(p,const_cast<t&>(e));
   }
   return p;
}

template<typename t>
inline const char *enc_dec(
   const char *p,
   multiset<t>&s,
   enc_dec_set_context<t> c = enc_dec_set_context<t>()
   ) {
   size_t sz;
   p = enc_dec(p,sz);
   while (sz--) {
      t temp;
      p = c(p,temp);
      s.insert(temp);
   }
   return p;
}

//
// Now the maps
//
template<typename k,typename v>
struct enc_dec_map_context {
   virtual size_t operator()(size_t      p,k& key,v& value) { return enc_dec_pair(p,key,value); }
   virtual char * operator()(char       *p,k& key,v& value) { return enc_dec_pair(p,key,value); }
   virtual const char *operator()(const char *p,k& key,v& value) { return enc_dec_pair(p,key,value); }
   virtual ~enc_dec_map_context() {}
};

template<typename k,typename v> 
inline size_t enc_dec(
   size_t p,
   std::map<k,v>&s,
   enc_dec_map_context<k,v> c = enc_dec_map_context<k,v>(),
   bool is_bounded_size = enc_dec_traits<k>::is_bounded_size && enc_dec_traits<v>::is_bounded_size
   ) {
   size_t sz;
   p = enc_dec(p,sz); 
   if (is_bounded_size) {
      p += s.size() * c(size_t(0),*(k *)0,*(v *)0);
   } else {
      for (auto &e : s) {
         p = c(p,const_cast<k&>(e.first),e.second);
      }
   }
   return p;    
}

template<typename k, typename v>
inline char *enc_dec(
   char *p,
   std::map<k,v>& s,
   enc_dec_map_context<k,v> c = enc_dec_map_context<k,v>() 
   ){
   size_t sz = s.size();
   p = enc_dec(p,sz);
   for (auto& e : s) {
      p = c(p,const_cast<k&>(e.first),e.second);
   }
   return p;
}

template<typename k, typename v>
inline const char *enc_dec(
   const char *p,
   std::map<k,v>&s,
   enc_dec_map_context<k,v> c = enc_dec_map_context<k,v>()
   ) {
   size_t sz;
   p = enc_dec(p,sz);
   while (sz--) {
      k key;
      v value;
      p = c(p,key,value);
      s[key] = value;
   }
   return p;
}

//
// And a multimap
//
template<typename k,typename v> 
inline size_t enc_dec(
   size_t p,
   std::multimap<k,v>&s,
   enc_dec_map_context<k,v> c = enc_dec_map_context<k,v>(),
   bool is_bounded_size = enc_dec_traits<k>::is_bounded_size && enc_dec_traits<v>::is_bounded_size
   ) {
   size_t sz;
   p = enc_dec(p,sz); 
   if (is_bounded_size) {
      p += s.size() * c(size_t(0),*(k *)0,*(v *)0);
   } else {
      for (auto &e : s) {
         p = c(p,const_cast<k&>(e.first),e.second);
      }
   }
   return p;    
}

template<typename k, typename v>
inline char *enc_dec(
   char *p,
   std::multimap<k,v>& s,
   enc_dec_map_context<k,v> c = enc_dec_map_context<k,v>() 
   ){
   size_t sz = s.size();
   p = enc_dec(p,sz);
   for (auto& e : s) {
      p = c(p,const_cast<k&>(e.first),e.second);
   }
   return p;
}

template<typename k, typename v>
inline const char *enc_dec(
   const char *p,
   std::multimap<k,v>&s,
   enc_dec_map_context<k,v> c = enc_dec_map_context<k,v>()
   ) {
   size_t sz;
   p = enc_dec(p,sz);
   while (sz--) {
      k key;
      v value;
      p = c(p,key,value);
      s.insert(key,value);
   }
   return p;
}

//
// Now a std::vector
//
template<typename e>
inline size_t enc_dec(
   size_t p,
   std::vector<e>& v,
   size_t (*delegate)(size_t,e&) = &enc_dec,
   bool is_bounded_size = enc_dec_traits<e>::is_bounded_size) {
   size_t size;
   p = enc_dec(p,size);
   if (is_bounded_size) {
      p += v.size() * (*delegate)(size_t(0),*(e *)0);
   } else {
     for (auto& o : v) p = (*delegate)(p,o);
   }
   return p;
}

template<typename e>
inline char *enc_dec(
   char *p,
   std::vector<e>& v,
   char * (*delegate)(char *,e&) = &enc_dec) {
   size_t size = v.size();
   p = enc_dec(p,size);
   for (auto& o : v) p = (*delegate)(p,o);
   return p;
}

template<typename e>
inline const char *enc_dec(
   const char * p,
   std::vector<e>& v,
   const char *(*delegate)(const char *,e&) = &enc_dec) {
   size_t len;
   p = enc_dec(p,len);
   v.reserve(len);
   while (len--) {
      e temp;
      p = (*delegate)(p,temp);
      v.push_back(temp);
   }
   return p;
}

//
// The default versions
//


//
// Specialized encode/decode for a single data type. These are invoked explicitly...
//
inline size_t enc_dec_lba(size_t p,int& lba) {
   return p + sizeof(lba); // Max....
}

inline char * enc_dec_lba(char *p,int& lba) {
   *p = 15;
   return p + 1; // blah blah
}

inline const char *enc_dec_lba(const char *p,int& lba) {
   lba = *p;
   return p+1;
}

//
// Specialized encode/decode for more sophisticated things primitives.
//
// Here's an example of a encode/decoder for a pair of fields
//
inline size_t enc_dec_range(size_t p,short& start,short& end) {
   return p + 2 * sizeof(short);
}

inline char *enc_dec_range(char *p, short& start, short& end) {
   short *s = (short *) p;
   s[0] = start;
   s[1] = end;
   return p + sizeof(short) * 2;
}

inline const char *enc_dec_range(const char *p,short& start, short& end) {
   start = *(short *)p;
   end   = *(short *)(p + sizeof(short));
   return p + 2*sizeof(short);
}

//
// Some C++ template wizardry to make the single encode/decode function possible.
//
enum SERIAL_TYPE {
   ESTIMATE,
   ENCODE,
   DECODE
};

template <enum SERIAL_TYPE s> struct serial_type;

template<> struct serial_type<ESTIMATE> { typedef size_t type; };
template<> struct serial_type<ENCODE>   { typedef char * type; };
template<> struct serial_type<DECODE>   { typedef const char *type; };

//
// This macros are the key, they connect the external non-member function to the correct member function.
// as well as defining some other machinery.
//

//
// Normally you use this macro.
//
#define DECLARE_ENC_DEC_CLASS(x)         DEFINE_ENC_DEC_CLASS_HELPER(x,false)

//
// Use this macro when you want to enable the optimization that containers of this type don't need
// to walk the container to estimate their size
//
#define DECLARE_ENC_DEC_CLASS_BOUNDED(x) DEFINE_ENC_DEC_CLASS_HELPER(x,true)

#define DEFINE_ENC_DEC_CLASS_HELPER(s,bounded) \
template<> struct enc_dec_traits<s> { enum { is_bounded_size = bounded }; }; \
inline size_t      enc_dec(size_t p, s &o)      { return o.enc_dec_member<ESTIMATE>(p); } \
inline char *      enc_dec(char *p , s &o)      { return o.enc_dec_member<ENCODE>(p); } \
inline const char *enc_dec(const char *p,s &o)  { return o.enc_dec_member<DECODE>(p); }

//
// Macros to help declare member enc_dec functions
//
// This macro is used for an inline definition of an enc_dec function OR to declare an out-of-line function
//
#define DECLARE_ENC_DEC_MEMBER_FUNCTION() \
   template<enum SERIAL_TYPE _stype> typename serial_type<_stype>::type enc_dec_member(typename serial_type<_stype>::type p) 

//
// This macro is used to define an out-of-line enc_dec function
//
#define DEFINE_ENC_DEC_MEMBER_FUNCTION(class) \
template<enum SERIAL_TYPE _stype> inline typename serial_type<_stype>::type class::enc_dec_member(typename serial_type<_stype>::type p) 

/*

------------------------------------- RECEIPES -----------------------------------------------------------

(1) If you define a new class/struct that you want to serialize OR provide enc_dec serialization to an existing structure

(a) Use DECLARE_ENC_DEC_MEMBER_FUNCTION macro to define a member function that details the serialization that you'll use.
(i) Populate the new member function with the appropriate calls to the different serializers
(b) Use either the DEFINE_ENC_DEC_CLASS_ or DECINE_ENC_DEC_CLASS_BOUNDED macros at the global level.

struct astruct {
   int a;
   set<int> b;
   int lba;
   short start,end;
   int signed_var_int;
   unsigned unsigned_var_int;
   long long signed_var_ll;
   unsigned long long unsigned_var_ll;
   std::map<std::string,std::string> m0;
   std::map<int,int> m1;
   //
   // <<<<< You need to provide this function just one.
   //
   DECLARE_ENC_DEC_MEMBER_FUNCTION() {
      p = enc_dec(p,a);
      p = enc_dec(p,b);
      p = enc_dec_lba(p,lba);
      p = enc_dec_range(p,start,end);
      p = enc_dec_varint(p,signed_var_int);
      p = enc_dec_varint(p,unsigned_var_int);
      p = enc_dec_varint(p,signed_var_ll);
      p = enc_dec_varint(p,unsigned_var_ll);
      p = enc_dec(p,m0);
      p = enc_dec(p,m1);
      return p;
   }
};
 
//
// This macro connects the global enc_dec to the member function.
// One of these per struct declaration
//
DEFINE_ENC_DEC_CLASS(astruct);


------------------------------------------------------------

(2) If you want to create a new format for an existing data type (primitive OR complex).

(a) define the three functional variants of enc_dec to perform the action: see enc_dec_varint or enc_dec_range for examples

------------------------------------------------------------

(3) How do I create a versioned serialization?

Here's the easy way :)

    struct versioned_object {
       type field1;
       type field2;
       type field3;        //   <<<<<------- Only present for version > 1
       DECLARE_ENC_DEC_MEMBER_FUNCTION() {
          //
          // Create a version field. This can be local OR could be part of the struct/class
          //  If you make the version field as part of the class, you get the advantage of being able to refer to it later
          //  however, you'll need to make sure it's initialized correctly for encode operations...
          //
          __u8 version = 2; // 2 is current version
          p = enc_dec(p,version);
          p = enc_dec(p,field1);
          p = enc_dec(p,field2);
          if (_stype != DECODE || verion > 1) p = enc_dec(p,field3);
          return p;
       }
    };




*/

/*

Now, glue this into the bufferlist world

The biggest issue betwee

*/

template<typename t>
inline void enc(bufferlist& b,const t& o) {
   //
   // Compute size, be sure to leave space for our overall size sentinal
   //
   size_t sz = enc_dec(sizeof(__le32),const_cast<t&>(o));
   //
   // Allocate the space and serialize the object, leave space for the sentinal
   //
   char *buffer_start = b.push_back(sz);
   char *data_start = buffer_start + sizeof(__le32);
   char *end = enc_dec(data_start,const_cast<t&>(o));
   assert(size_t(end-buffer_start) <= sz); // If you fail here, you've lied about an encoded size somewhere and you've corrupted memory!!!!
   
   *(__le32 *)buffer_start = __le32(end-data_start);

   //
   // give back and unused space
   //
   b.pop_back(sz - (end-buffer_start));
}

//
// Helper for decode
//
inline const char *straighten_iterator(bufferlist::iterator& i,size_t wanted,unique_ptr<char>& temp_buffer) {
   const char *p;
   size_t actual = i.get_ptr_and_advance(wanted,&p);
   if (actual == wanted) return p; // done :)
   //
   // Ugly case :(
   //
   temp_buffer.reset(new char[wanted]);
   memcpy(temp_buffer.get(),p,actual);
   i.copy(wanted - actual,temp_buffer.get() + actual);
   return temp_buffer.get();
}

template<typename t>
inline void dec(bufferlist::iterator& i,t& o) {
   unique_ptr<char> buf0;
   unique_ptr<char> buf1; // Incase we have to allocate a buffer
   //
   // Read the sentinal
   //
   const char *p = straighten_iterator(i,sizeof(__le32),buf0);
   size_t buf_sz = *(__le32 *)p;
   //
   // Now the data itself
   //
   p = straighten_iterator(i,buf_sz,buf1);
   const char *end = enc_dec(p,o);
   if (end != (p + buf_sz)) {
      throw buffer::malformed_input(__PRETTY_FUNCTION__);
   }
}

#endif

