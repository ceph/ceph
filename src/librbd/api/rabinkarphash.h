#ifndef KARPRABINHASH
#define KARPRABINHASH


#include "characterhash.h"


/**
* This is a randomized version of the Karp-Rabin hash function.
* Each instance is a rolling hash function meant to hash streams of characters.
* Each new instance of this class comes with new random keys.
*
* Recommended usage to get L-bit hash values over n-grams:
*        KarpRabinHash<> hf(n,L );
*        for(uint32 k = 0; k<n;++k) {
*                  unsigned char c = ... ; // grab some character
*                  hf.eat(c); // feed it to the hasher
*        }
*        while(...) { // go over your string
*           hf.hashvalue; // at all times, this contains the hash value
*           unsigned char c = ... ;// points to the next character
*           unsigned char out = ...; // character we want to forget
*           hf.update(out,c); // update hash value
*        }
*/
template <typename hashvaluetype = uint32, typename chartype =  unsigned char>
class KarpRabinHash {

public:
    // myn is the length of the sequences, e.g., 3 means that you want to hash sequences of 3 characters
    // mywordsize is the number of bits you which to receive as hash values, e.g., 19 means that the hash values are 19-bit integers
    KarpRabinHash(int myn, int mywordsize=19) :  hashvalue(0),n(myn),
        wordsize(mywordsize),
        hasher( maskfnc<hashvaluetype>(wordsize)),
        HASHMASK(maskfnc<hashvaluetype>(wordsize)),BtoN(1) {
        for (int i=0; i < n ; ++i) {
            BtoN *= B;
            BtoN &= HASHMASK;
        }
    }

    // this is a convenience function, use eat,update and .hashvalue to use as a rolling hash function
    template<class container>
    hashvaluetype  hash(container & c) {
        hashvaluetype answer(0);
        for(uint k = 0; k<c.size(); ++k) {
            hashvaluetype x(1);
            for(uint j = 0; j< c.size()-1-k; ++j) {
                x= (x * B) & HASHMASK;
            }
            x= (x * hasher.hashvalues[c[k]]) & HASHMASK;
            answer=(answer+x) & HASHMASK;
        }
        return answer;
    }

    // add inchar as an input, this is used typically only at the start
    // the hash value is updated to that of a longer string (one where inchar was appended)
    void eat(chartype inchar) {
        hashvalue = (B*hashvalue +  hasher.hashvalues[inchar] )& HASHMASK;
    }

    // add inchar as an input and remove outchar, the hashvalue is updated
    // this function can be used to update the hash value from the hash value of [outchar]ABC to the hash value of ABC[inchar]
    void update(chartype outchar, chartype inchar) {
        hashvalue = (B*hashvalue +  hasher.hashvalues[inchar] - BtoN *  hasher.hashvalues[outchar]) & HASHMASK;
    }


    hashvaluetype hashvalue;
    int n;
    const int wordsize;
    CharacterHash<hashvaluetype,chartype> hasher;
    const hashvaluetype HASHMASK;
    hashvaluetype BtoN;
    static const hashvaluetype B=37;


};


#endif


