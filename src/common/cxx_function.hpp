// cxx_function.hpp: major evolution for std::function
// Copyright 2015 by David Krauss.
// This source is released under the MIT license, http://opensource.org/licenses/MIT

#ifndef INCLUDED_CXX_FUNCTION_HPP
#define INCLUDED_CXX_FUNCTION_HPP

#include <cassert>
#include <cstring>
#include <exception>
#include <functional> // for std::bad_function_call and std::mem_fn
#include <memory>
#include <new>
#include <tuple>
#include <type_traits>
#include <utility>

namespace cxx_function {

// Dispatch tag for in-place construction, for when explicit template arguments are unavailable (e.g. constructor calls).
template< typename >
struct in_place_t {};

#if __cplusplus >= 201402
template< typename t >
constexpr in_place_t< t > in_place = {};
#endif

namespace impl {

#if ! __clang__ && __GNUC__ < 5
#   define is_trivially_move_constructible has_trivial_copy_constructor
#   define is_trivially_copy_constructible has_trivial_copy_constructor

#   define deprecated(MSG) __attribute__((deprecated (MSG)))

#   define OLD_GCC_FIX(...) __VA_ARGS__
#   define OLD_GCC_SKIP(...)
#else
#   define deprecated(MSG) [[deprecated (MSG)]]

#   define OLD_GCC_FIX(...)
#   define OLD_GCC_SKIP(...) __VA_ARGS__
#endif

#define UNPACK(...) __VA_ARGS__
#define IGNORE(...)

#define DISPATCH_CQ( MACRO, UNSAFE, QUALS ) MACRO( QUALS, UNSAFE ) MACRO( const QUALS, IGNORE )
#define DISPATCH_CV( MACRO, UNSAFE, QUALS ) DISPATCH_CQ( MACRO, UNSAFE, QUALS ) DISPATCH_CQ( MACRO, IGNORE, volatile QUALS )

// Apply a given macro over all reference qualifications.
#define DISPATCH_CVREFQ( MACRO, QUALS ) DISPATCH_CV( MACRO, IGNORE, & QUALS ) DISPATCH_CV( MACRO, IGNORE, && QUALS )

// Apply a given macro over all type qualifications.
#define DISPATCH_ALL( MACRO ) DISPATCH_CV( MACRO, UNPACK, ) DISPATCH_CVREFQ( MACRO, )

// Convert a member function signature to its free invocation counterpart.
template< typename sig >
struct implicit_object_to_parameter;

template< typename t >
struct add_reference
    { typedef t & type; };
template< typename t >
struct add_reference< t && >
    { typedef t && type; };

struct erasure_handle {}; // The member function should belong to a class derived from this base.

#define TYPE_CONVERT_CASE( QUALS, UNSAFE ) \
template< typename ret, typename ... arg > \
struct implicit_object_to_parameter< ret( arg ... ) QUALS > \
    { typedef ret type( add_reference< erasure_handle QUALS >::type, arg ... ); };
DISPATCH_ALL( TYPE_CONVERT_CASE )
#undef TYPE_CONVERT_CASE

// Apply given cv-qualifiers and reference category to a new type.
template< typename source, typename target >
struct transfer_qualifiers;
#define TRANSFER_QUALS_CASE( QUALS, UNSAFE ) \
template< typename source, typename target > \
struct transfer_qualifiers< source QUALS, target > \
    { typedef target QUALS type; };
DISPATCH_CVREFQ( TRANSFER_QUALS_CASE, )
#undef TRANSFER_QUALS_CASE

// Apply cv-qualifiers and reference category from a result of implicit_object_to_parameter to another type.
template< typename sig, typename target >
struct transfer_param_qualifiers;
template< typename ret, typename source, typename ... arg, typename target >
struct transfer_param_qualifiers< ret( source, arg ... ), target >
    { typedef typename transfer_qualifiers< source, target >::type type; };

/* Implement a vtable using metaprogramming. Why?
    1. Implement without polymorphic template instantiations (would need 2N of them).
    2. Eliminate overhead and ABI issues associated with RTTI and weak linkage.
    3. Allow static data entries as well as functions.
    
    The table is stored as a std::tuple of function pointers and type_info*'s.
    Entries that would be trivial or useless may be set to nullptr.
*/
enum class dispatch_slot {
    destructor,
    move_constructor_destructor,
    copy_constructor,
    target_access,
    target_type,
    allocator_type,
    
    base_index
};
constexpr int operator + ( dispatch_slot e ) { return static_cast< int >( e ); }

// Generic "virtual" functions to manage the wrapper payload lifetime.
template< typename derived >
struct erasure_special {
    static void destroy( erasure_handle & self, void * ) noexcept
        { static_cast< derived & >( self ). ~ derived(); }
    static void move( erasure_handle && self, void * dest, void *, void * ) {
        new (dest) derived( std::move( static_cast< derived & >( self ) ) );
        destroy( self, {} );
    }
    static void copy( erasure_handle const & self, void * dest, void * )
        { new (dest) derived( static_cast< derived const & >( self ) ); }
};

// These accessors generate "vtable" entries, but avoid instantiating functions that do not exist or would be trivial.
// Most are specialized for allocator_erasure.
template< typename >
struct is_allocator_erasure : std::false_type {};

template< typename derived >
constexpr typename std::enable_if<
    ! std::is_trivially_destructible< derived >::value
    || is_allocator_erasure< derived >::value >::type
( * erasure_destroy() ) ( erasure_handle &, void * )
    { return & derived::destroy; }
template< typename derived >
constexpr typename std::enable_if<
    std::is_trivially_destructible< derived >::value
    && ! is_allocator_erasure< derived >::value >::type
( * erasure_destroy() ) ( erasure_handle &, void * )
    { return nullptr; }

template< typename erasure >
struct erasure_trivially_movable : std::integral_constant< bool,
    std::is_trivially_move_constructible< erasure >::value
    && std::is_trivially_destructible< erasure >::value > {};

template< typename derived >
constexpr typename std::enable_if< ! erasure_trivially_movable< derived >::value >::type
( * erasure_move() ) ( erasure_handle &&, void *, void *, void * )
    { return & derived::move; }
template< typename derived >
constexpr typename std::enable_if< erasure_trivially_movable< derived >::value >::type
( * erasure_move() ) ( erasure_handle &&, void *, void *, void * )
    { return nullptr; }

template< typename erasure >
struct erasure_nontrivially_copyable : std::integral_constant< bool,
    std::is_copy_constructible< erasure >::value
    && ! std::is_trivially_copy_constructible< erasure >::value > {};

template< typename derived, typename enable >
constexpr typename std::enable_if< std::enable_if< enable::value, erasure_nontrivially_copyable< derived > >::type::value >::type
( * erasure_copy( enable * ) ) ( erasure_handle const &, void *, void * )
    { return & derived::copy; }
template< typename derived >
constexpr void ( * erasure_copy( ... ) ) ( erasure_handle const &, void *, void * )
    { return nullptr; }

template< typename derived >
constexpr typename std::enable_if< is_allocator_erasure< derived >::value,
std::type_info const * >::type erasure_allocator_type()
    { return & typeid (typename derived::common_allocator); }

template< typename derived >
constexpr typename std::enable_if< ! is_allocator_erasure< derived >::value,
std::type_info const * >::type erasure_allocator_type()
    { return nullptr; }


template< typename >
struct const_unsafe_case; // internal tag for function signatures introduced for backward compatibility of const-qualified access

// Table generator.
// typename copyable enables copyability, but trivially copyable and noncopyable tables look the same. The default is trivially-copyable.
template< typename erasure, typename copyable = std::false_type, typename = typename erasure::erasure_base >
struct erasure_table;

// "Abstract" base class for the island inside the wrapper class, e.g. std::function.
// This must appear first in the most-derived class layout.
template< typename ... free >
struct erasure_base : erasure_handle {
    typedef std::tuple<
        void (*)( erasure_handle &, void * alloc ), // destructor
        void (*)( erasure_handle &&, void * dest, void * source_alloc, void * dest_alloc ), // move constructor + destructor
        void (*)( erasure_handle const &, void * dest, void * alloc ), // copy constructor
        
        void const * (*)( erasure_handle const & ), // target access
        std::type_info const &, // target_type
        std::type_info const *, // allocator_type
        
        free * ... // dispatchers
    > dispatch_table;
    
    dispatch_table const & table;
    
    template< typename derived, typename copyable >
    constexpr erasure_base( derived *, copyable )
        : table( erasure_table< derived, copyable >::value ) {}
};
// Generate "vtable".
template< typename erasure, typename copyable, typename ... free >
struct erasure_table< erasure, copyable, erasure_base< free ... > >
    { static typename erasure::dispatch_table value; };
template< typename erasure, typename copyable, typename ... free >
typename erasure::dispatch_table erasure_table< erasure, copyable, erasure_base< free ... > >::value {
    erasure_destroy< erasure >(),
    erasure_move< erasure >(),
    erasure_copy< erasure >( static_cast< copyable * >( nullptr ) ),
    & erasure::target_access,
    typeid (typename erasure::target_type),
    erasure_allocator_type< erasure >(),
    static_cast< free * >( & erasure::template call< typename transfer_param_qualifiers< free, erasure >::type > ) ...
};

// Implement the uninitialized state.
template< typename ... sig >
struct null_erasure
    : erasure_base< sig ... > // "vtable" interface class
    , erasure_special< null_erasure< sig ... > > { // generic implementations of "virtual" functions
    typedef void target_type;
    
    // The const qualifier is bogus. Rather than type-erase an identical non-const version, let the wrapper do a const_cast.
    static void const * target_access( erasure_handle const & ) { return nullptr; } // target<void>() still returns nullptr.
    
    null_erasure() noexcept
        : null_erasure::erasure_base( this, std::false_type{} ) {} // Initialize own "vtable pointer" at runtime.
    
    template< typename, typename ret, typename erasure_base_ref, typename ... arg >
    static ret call( erasure_base_ref, arg ... )
        { throw std::bad_function_call{}; }
};

// Implement erasures of objects which are small and have a well-defined call operator.
template< typename in_target_type, typename ... sig >
struct local_erasure
    : erasure_base< sig ... >
    , erasure_special< local_erasure< in_target_type, sig ... > > {
    typedef in_target_type target_type;
    target_type target;
    
    static void const * target_access( erasure_handle const & self )
        { return & static_cast< local_erasure const & >( self ).target; }
    
    template< typename copyable, typename ... arg >
    local_erasure( copyable, arg && ... a )
        : local_erasure::erasure_base( this, copyable{} )
        , target( std::forward< arg >( a ) ... ) {}
    
    template< typename qualified, typename ret, typename erasure_base_ref, typename ... arg >
    static ret call( erasure_base_ref self, arg ... a )
        // Directly call the name "target," not a reference, to support devirtualization.
        { return static_cast< qualified >( self ).target( std::forward< arg >( a ) ... ); }
};

// Implement erasures of pointer-to-members, which need std::mem_fn instead of a direct call.
template< typename in_target_type, typename ... sig >
struct ptm_erasure
    : erasure_base< sig ... >
    , erasure_special< ptm_erasure< in_target_type, sig ... > > {
    typedef in_target_type target_type;
    target_type target; // Do not use mem_fn here...
    
    static void const * target_access( erasure_handle const & self )
        { return & static_cast< ptm_erasure const & >( self ).target; } // ... because the user can get read/write access to the target object.
    
    ptm_erasure( target_type a )
        : ptm_erasure::erasure_base( this, std::false_type{} )
        , target( a ) {}
    
    template< typename qualified, typename ret, typename erasure_base_ref, typename ... arg >
    static ret call( erasure_base_ref self, arg ... a )
        { return std::mem_fn( static_cast< qualified >( self ).target )( std::forward< arg >( a ) ... ); }
};

// Implement erasures of objects that cannot be stored inside the wrapper.
/*  This does still store the allocator and pointer in the wrapper. A more general case should be added.
    However, there is a conundrum in rebinding an allocator to an instance of itself.
    Also, it's not clear that a native pointer will always be stable, as opposed to a fancy pointer.
    Fancy pointers exceeding the wrapper storage, with varying underlying referent storage, are another conundrum. */
// Use Allocator<char> as a common reference point, for the typeid operator and the instance in function_container.
// (The instance in the erasure object is always a bona fide Allocator<T>, though.)
template< typename allocator >
using common_allocator_rebind = typename std::allocator_traits< allocator >::template rebind_alloc< char >;

template< typename t, typename = void, typename = void >
struct is_always_equal : std::false_type {};
template< typename t >
struct is_always_equal< t, void, typename std::enable_if< std::allocator_traits< t >::is_always_equal::value >::type >
    : std::true_type {};
template< typename t, typename v >
struct is_always_equal< t, v, typename std::enable_if< decltype (std::declval< t >() == std::declval< t >())::value >::type >
    : std::true_type {};
template< typename t >
struct is_always_equal< std::allocator< t > >
    : std::true_type {};

template< typename allocator, typename in_target_type, typename ... sig >
struct allocator_erasure
    : erasure_base< sig ... >
    , allocator { // empty base class optimization (EBCO)
    typedef std::allocator_traits< allocator > allocator_traits;
    typedef common_allocator_rebind< allocator > common_allocator;
    
    typedef in_target_type target_type;
    typename allocator_traits::pointer target;
    
    allocator & alloc() { return static_cast< allocator & >( * this ); }
    allocator const & alloc() const { return static_cast< allocator const & >( * this ); }
    target_type * target_address() { return std::addressof( * target ); }
    static void const * target_access( erasure_handle const & self )
        { return std::addressof( * static_cast< allocator_erasure const & >( self ).target ); }
    
    template< typename ... arg >
    void construct_safely( arg && ... a ) try {
        allocator_traits::construct( alloc(), target_address(), std::forward< arg >( a ) ... );
    } catch (...) {
        allocator_traits::deallocate( alloc(), target, 1 ); // Does not throw according to [allocator.requirements] §17.6.3.5 and DR2384.
        throw;
    } // The wrapper allocator instance cannot be updated following a failed initialization because the erasure allocator is already gone.
    
    allocator_erasure( allocator_erasure && ) = default; // Called by move( true_type{}, ... ) when allocator or pointer is nontrivially movable.
    allocator_erasure( allocator_erasure const & ) = delete;
    
    template< typename copyable, typename ... arg >
    allocator_erasure( copyable, std::allocator_arg_t, allocator const & in_alloc, arg && ... a )
        : allocator_erasure::erasure_base( this, copyable{} )
        , allocator( in_alloc )
        , target( allocator_traits::allocate( alloc(), 1 ) )
        { construct_safely( std::forward< arg >( a ) ... ); }
    
    // Copy or move. Moving only occurs to a different pool.
    template< typename source >
    allocator_erasure( std::allocator_arg_t, allocator const & dest_allocator, source && o )
        : allocator_erasure::erasure_base( o )
        , allocator( dest_allocator )
        , target( allocator_traits::allocate( alloc(), 1 ) )
        { construct_safely( std::forward< typename transfer_qualifiers< source &&, target_type >::type >( * o.target ) ); }
    
    void move( std::true_type, void * dest, void *, void * ) noexcept { // Call ordinary move constructor.
        new (dest) allocator_erasure( std::move( * this ) ); // Move the pointer, not the object. Don't call the allocator at all.
        this-> ~ allocator_erasure();
    }
    void move( std::false_type, void * dest, void * source_allocator_v, void * dest_allocator_v ) {
        auto * dest_allocator_p = static_cast< common_allocator * >( dest_allocator_v ); // The wrapper verified the safety of this using typeid.
        if ( ! dest_allocator_p || * dest_allocator_p == alloc() ) {
            move( std::true_type{}, dest, source_allocator_v, dest_allocator_v ); // same pool
        } else { // different pool
            auto & e = * new (dest) allocator_erasure( std::allocator_arg, static_cast< allocator >( * dest_allocator_p ), // Reallocate.
                std::move_if_noexcept( * this ) ); // Protect user against their own throwing move constructors.
            * dest_allocator_p = e.alloc(); // Update the wrapper allocator instance with the new copy, potentially updated by the new allocation.
            destroy( * this, source_allocator_v );
        }
    }
    // [*_]allocator_v points to the wrapper allocator instance, if any.
    static void move( erasure_handle && self_base, void * dest, void * source_allocator_v, void * dest_allocator_v ) {
        auto & self = static_cast< allocator_erasure & >( self_base );
        // is_always_equal is usually false here, because it correlates with triviality which short-circuits this function.
        std::move( self ).move( is_always_equal< allocator >{}, dest, source_allocator_v, dest_allocator_v );
    }
    static void copy( erasure_handle const & self_base, void * dest, void * dest_allocator_v ) {
        auto * dest_allocator_p = static_cast< common_allocator * >( dest_allocator_v );
        auto & self = static_cast< allocator_erasure const & >( self_base );
        // Structure the control flow differently to avoid instantiating the copy constructor.
        allocator const & dest_allocator = dest_allocator_p?
            static_cast< allocator const & >( * dest_allocator_p ) : self.alloc();
        auto & e = * new (dest) allocator_erasure( std::allocator_arg, dest_allocator, self );
        if ( dest_allocator_p ) * dest_allocator_p = static_cast< common_allocator const & >( e.alloc() ); // Likewise, update the wrapper allocator instance with the new copy.
    }
    static void destroy( erasure_handle & self_base, void * allocator_v ) noexcept {
        auto & self = static_cast< allocator_erasure & >( self_base );
        allocator_traits::destroy( self.alloc(), self.target_address() );
        allocator_traits::deallocate( self.alloc(), self.target, 1 );
        if ( allocator_v ) * static_cast< common_allocator * >( allocator_v ) = static_cast< common_allocator const & >( self.alloc() );
        self. ~ allocator_erasure();
    }
    
    template< typename qualified, typename ret, typename erasure_base_ref, typename ... arg >
    static ret call( erasure_base_ref self_base, arg ... a ) {
        auto && self = static_cast< qualified >( self_base );
        return std::forward< typename transfer_qualifiers< qualified, target_type >::type >( * self.target )( std::forward< arg >( a ) ... );
    }
};

template< typename allocator, typename target_type, typename ... sig >
struct is_allocator_erasure< allocator_erasure< allocator, target_type, sig ... > > : std::true_type {};

template< typename allocator, typename target_type, typename ... sig >
struct erasure_trivially_movable< allocator_erasure< allocator, target_type, sig ... > > : std::integral_constant< bool,
    std::is_trivially_move_constructible< allocator_erasure< allocator, target_type, sig ... > >::value
    && std::is_trivially_destructible< allocator_erasure< allocator, target_type, sig ... > >::value
    && is_always_equal< allocator >::value > {};

template< typename allocator, typename target_type, typename ... sig >
struct erasure_nontrivially_copyable< allocator_erasure< allocator, target_type, sig ... > >
    : std::is_copy_constructible< target_type > {};


// Metaprogramming for checking a potential target against a list of signatures.
template< bool ... cond >
struct logical_intersection
    : std::true_type {};
template< bool ... cond >
struct logical_intersection< true, cond ... >
    : logical_intersection< cond ... >::type {};
template< bool ... cond >
struct logical_intersection< false, cond ... >
    : std::false_type {};

template< typename t, typename sig, typename = void >
struct is_callable : std::false_type {};

#define IS_CALLABLE_CASE( QUALS, UNSAFE ) \
template< typename t, typename ret, typename ... arg > \
struct is_callable< t, ret( arg ... ) QUALS, \
    typename std::enable_if< std::is_convertible< \
        typename std::result_of< typename add_reference< t QUALS >::type ( arg ... ) >::type \
    , ret >::value >::type > \
    : std::true_type {};

DISPATCH_ALL( IS_CALLABLE_CASE )
#undef IS_CALLABLE_CASE

template< typename sig >
struct is_callable< std::nullptr_t, sig >
    : std::true_type {};

template< typename ... sig >
struct is_all_callable {
    template< typename t >
    using temp = typename logical_intersection< is_callable< t, sig >::value ... >::type;
    
    typedef std::false_type copies;
};

template< typename self, typename ... sig >
struct is_copyable_all_callable {
    template< typename t, typename = void >
    struct temp : std::integral_constant< bool,
        std::is_copy_constructible< t >::value
        && is_all_callable< sig ... >::template temp< t >::value > {};
    
    template< typename v > // Presume that self is a copyable wrapper, since that is what uses this metafunction.
    struct temp< self, v > : std::true_type {};
    
    typedef std::true_type copies;
};

// Map privileged types to noexcept specifications.
template< typename source >
struct is_noexcept_erasable : std::false_type {};
template<>
struct is_noexcept_erasable< std::nullptr_t > : std::true_type {};
template< typename t >
struct is_noexcept_erasable< t * > : std::true_type {};
template< typename t, typename c >
struct is_noexcept_erasable< t c::* > : std::true_type {};
template< typename t >
struct is_noexcept_erasable< std::reference_wrapper< t > > : std::true_type {};

// Termination of the recursive template generated by WRAPPER_CASE.
template< typename derived, std::size_t n, typename ... sig >
struct wrapper_dispatch {
    static_assert ( sizeof ... (sig) == 0, "An unsupported function signature was detected." );
    void operator () ( wrapper_dispatch ) = delete; // Feed the "using operator ();" or "using call;" declaration in next derived class.
};

// Generate the wrapper dispatcher by brute-force specialization over qualified function types.

// This macro generates a recursive template handling one type qualifier sequence, e.g. "volatile &" or "const."
// The final product converts a sequence of qualified signatures into an overload set, potentially with special cases for signatures of no qualification.
#define WRAPPER_CASE( \
    QUALS, /* The type qualifiers for this case. */ \
    UNSAFE /* UNPACK if there are no qualifiers, IGNORE otherwise. Supports deprecated const-qualified access. */ \
) \
template< typename derived, std::size_t table_index, typename ret, typename ... arg, typename ... sig > \
struct wrapper_dispatch< derived, table_index, ret( arg ... ) QUALS, sig ... > \
    : wrapper_dispatch< derived, table_index+1, sig ... \
        UNSAFE (, const_unsafe_case< ret( arg ... ) >) > { \
    using wrapper_dispatch< derived, table_index+1, sig ... \
        UNSAFE (, const_unsafe_case< ret( arg ... ) >) >::operator (); \
    ret operator () ( arg ... a ) QUALS { \
        auto && self = static_cast< typename add_reference< derived QUALS >::type >( * this ); \
        return std::get< + dispatch_slot::base_index + table_index >( self.erasure().table ) \
            ( std::forward< decltype (self) >( self ).erasure(), std::forward< arg >( a ) ... ); \
    } \
};
DISPATCH_ALL( WRAPPER_CASE )
#undef WRAPPER_CASE

// Additionally implement the legacy casting away of const, but with a warning.
template< typename derived, std::size_t n, typename ret, typename ... arg, typename ... more >
struct wrapper_dispatch< derived, n, const_unsafe_case< ret( arg ... ) >, more ... >
    : wrapper_dispatch< derived, n, more ... > {
    using wrapper_dispatch< derived, n, more ... >::operator ();
    deprecated( "It is unsafe to call a std::function of non-const signature through a const access path." )
    ret operator () ( arg ... a ) const {
        return const_cast< derived & >( static_cast< derived const & >( * this ) )
            ( std::forward< arg >( a ) ... );
    }
};

template< typename ... sig >
class wrapper_base
    : public wrapper_dispatch< wrapper_base< sig ... >, 0, sig ... > {
    template< typename, typename, typename ... >
    friend class wrapper;
    typedef std::aligned_storage< sizeof (void *[3]) >::type effective_storage_type;
protected:
    std::aligned_storage< sizeof (void *[4]), alignof(effective_storage_type) >::type storage;
    void * storage_address() { return & storage; }
    
    // init and destroy enter or recover from invalid states.
    // They get on the right side of [basic.life]/7.4, but mind the exceptions.
    
    // Default, move, and copy construction.
    void init( in_place_t< std::nullptr_t >, std::nullptr_t ) noexcept
        { new (storage_address()) null_erasure< typename implicit_object_to_parameter< sig >::type ... >; }
    
    // Pointers are local callables.
    template< typename t >
    void init( in_place_t< t * >, t * p ) noexcept {
        if ( p ) new (storage_address()) local_erasure< t *, typename implicit_object_to_parameter< sig >::type ... >( std::false_type{}, p );
        else init( in_place_t< std::nullptr_t >{}, nullptr );
    }
    // PTMs are like local callables.
    template< typename t, typename c >
    void init( in_place_t< t c::* >, t c::* ptm ) noexcept {
        if ( ptm ) new (storage_address()) ptm_erasure< t c::*, typename implicit_object_to_parameter< sig >::type ... >( ptm );
        else init( in_place_t< std::nullptr_t >{}, nullptr );
    }
    
    // Implement erasure type verification for always-local targets without touching RTTI.
    bool verify_type_impl( void * ) const noexcept
        { return & erasure().table == & erasure_table< null_erasure< typename implicit_object_to_parameter< sig >::type ... > >::value; }
    
    template< typename t >
    bool verify_type_impl( std::reference_wrapper< t > * ) const noexcept {
        return & erasure().table
            == & erasure_table< local_erasure< std::reference_wrapper< t >, typename implicit_object_to_parameter< sig >::type ... > >::value;
    }
    template< typename t >
    bool verify_type_impl( t ** ) const noexcept
        { return & erasure().table == & erasure_table< local_erasure< t *, typename implicit_object_to_parameter< sig >::type ... > >::value; }
    
    template< typename t, typename c >
    bool verify_type_impl( t c::** ) const noexcept
        { return & erasure().table == & erasure_table< ptm_erasure< t c::*, typename implicit_object_to_parameter< sig >::type ... > >::value; }
    
    // User-defined class types are never guaranteed to be local. There could exist some allocator for which uses_allocator is true.
    // RTTI could be replaced here by a small variable template linked from the table. Since we need it anyway, just use RTTI.
    template< typename want >
    bool verify_type_impl( want * ) const noexcept
        { return target_type() == typeid (want); }
public:
    #define ERASURE_ACCESS( QUALS, UNSAFE ) \
        erasure_base< typename implicit_object_to_parameter< sig >::type ... > QUALS erasure() QUALS \
            { return reinterpret_cast< erasure_base< typename implicit_object_to_parameter< sig >::type ... > QUALS >( storage ); }
    DISPATCH_CVREFQ( ERASURE_ACCESS, )
    #undef ERASURE_ACCESS
    
    std::type_info const & target_type() const noexcept
        { return std::get< + dispatch_slot::target_type >( erasure().table ); }
    
    template< typename want >
    bool verify_type() const noexcept {
        static_assert ( ! std::is_reference< want >::value, "function does not support reference-type targets." );
        static_assert ( ! std::is_const< want >::value && ! std::is_volatile< want >::value, "function does not support cv-qualified targets." );
        return verify_type_impl( (want *) nullptr );
    }
    template< typename want >
    bool verify_type() const volatile noexcept
        { return const_cast< wrapper_base const * >( this )->verify_type< want >(); }
    
    void const * complete_object_address() const noexcept
        { return std::get< + dispatch_slot::target_access >( erasure().table ) ( erasure() ); }
    void const volatile * complete_object_address() const volatile noexcept
        { return const_cast< wrapper_base const * >( this )->complete_object_address(); }
    
    template< typename want >
    want const * target() const noexcept {
        if ( ! verify_type< want >() ) return nullptr;
        return static_cast< want const * >( std::get< + dispatch_slot::target_access >( erasure().table ) ( erasure() ) );
    }
    template< typename want >
    want * target() noexcept
        { return const_cast< want * >( static_cast< wrapper_base const & >( * this ).target< want >() ); }
    
    explicit operator bool () const volatile noexcept
        { return ! verify_type< void >(); }
};

struct allocator_mismatch_error : std::exception // This should be implemented in a .cpp file, but stay header-only for now.
    { virtual char const * what() const noexcept override { return "An object could not be transferred into an incompatible memory allocation scheme."; } };

// Mix-in a persistent allocator to produce a [unique_]function_container.
template< typename allocator >
class wrapper_allocator
    : common_allocator_rebind< allocator > {
    typedef std::allocator_traits< common_allocator_rebind< allocator > > allocator_traits;
    
    void do_swap( std::true_type, wrapper_allocator & o ) noexcept
        { using std::swap; swap( actual_allocator(), o.actual_allocator() ); }
    void do_swap( std::false_type, wrapper_allocator & o ) noexcept
        { assert ( actual_allocator() == o.actual_allocator() && "Cannot swap containers while not-swapping their unequal allocators." ); } // Roughly match libc++.
protected:
    wrapper_allocator( wrapper_allocator && ) /*noexcept*/ = default;
    
    /*  Do select_on_container_copy_construction here, although the "container" is really represented
        by the set of all equivalent allocators in various independent target objects. */
    wrapper_allocator( wrapper_allocator const & o )
        : common_allocator_rebind< allocator >( allocator_traits::select_on_container_copy_construction( o ) )
        {}
    
    // Likewise the rationale for POCMA, POCCA, and POCS. They propagate freely between targets, not between containers.
    wrapper_allocator & operator = ( wrapper_allocator && o ) noexcept {
        if ( typename allocator_traits::propagate_on_container_move_assignment() ) actual_allocator() = std::move( o.actual_allocator() );
        return * this;
    }
    wrapper_allocator & operator = ( wrapper_allocator const & o ) noexcept {
        if ( typename allocator_traits::propagate_on_container_copy_assignment() ) actual_allocator() = o;
        return * this;
    }
    void swap( wrapper_allocator & o ) noexcept
        { do_swap( allocator_traits::propagate_on_container_swap(), o ); }
    static constexpr bool noexcept_move_assign = allocator_traits::propagate_on_container_move_assignment::value || is_always_equal< allocator >::value;
    static constexpr bool noexcept_move_adopt = false; // Adoption may produce an allocator_mismatch_error.
    
    template< typename derived, typename t >
    derived reallocate( t && o )
        { return { std::allocator_arg, actual_allocator(), std::forward< t >( o ) }; }
    
    common_allocator_rebind< allocator > & actual_allocator()
        { return * this; }
    
    // Determine whether to use the stored allocator for an initialization or assignment.
    template< typename erasure_base > // Templating this is a bit bogus, since it only uses the "vtable header," but the type system doesn't know that.
    common_allocator_rebind< allocator > * compatible_allocator( erasure_base const & e ) {
        std::type_info const * type = std::get< + dispatch_slot::allocator_type >( e.table ); // Get dynamic type of the source allocator.
        if ( type == nullptr ) return nullptr; // It's a local erasure. Allocators don't apply. Avoid the potentially expensive type_info::operator==.
        if ( * type != typeid (actual_allocator()) ) throw allocator_mismatch_error{}; // Oh no!
        return & actual_allocator();
    }
    common_allocator_rebind< allocator > * any_allocator()
        { return & actual_allocator(); }
    
    typedef allocator allocator_t;
public: // Include the container-like interface.
    wrapper_allocator() = default;
    wrapper_allocator( std::allocator_arg_t, allocator const & in_alloc ) noexcept
        : common_allocator_rebind< allocator >( in_alloc ) {}
    
    allocator get_allocator() const
        { return * this; }
};

// Stub-out the container support.
class wrapper_no_allocator {
protected:
    static constexpr bool noexcept_move_assign = true;
    static constexpr bool noexcept_move_adopt = true;
    
    template< typename, typename t >
    t && reallocate( t && o )
        { return std::forward< t >( o ); }
    
    template< typename derived, typename t >
    derived reallocate( t const & o )
        { return o; }
    
    // This defines the default allocation of targets generated by non-container functions.
    typedef wrapper_no_allocator allocator_t;
    std::allocator< char > actual_allocator() { return {}; }
    
    template< typename erasure_base >
    static constexpr void * compatible_allocator( erasure_base const & )
        { return nullptr; }
    static constexpr void * any_allocator()
        { return nullptr; }
    
    wrapper_no_allocator() = default;
     // This is used internally but not publicly accessible. (It's publicly *visible* because inheriting constructors don't work very well.)
    template< typename ignored >
    wrapper_no_allocator( std::allocator_arg_t, ignored const & ) noexcept {}
    
    void swap( wrapper_no_allocator & ) {}
};

template< typename target_policy, typename allocator_manager, typename ... sig >
class wrapper
    : public allocator_manager
    , wrapper_base< sig ... > {
    template< typename, typename, typename ... >
    friend class wrapper;
    
    using wrapper::wrapper_base::erasure;
    using wrapper::wrapper_base::storage;
    using wrapper::wrapper_base::storage_address;
    using wrapper::wrapper_base::init;
    
    // Queries on potential targets.
    template< typename target >
    using is_targetable = typename target_policy::template temp< target >;
    
    template< typename source, typename allocator = typename allocator_manager::allocator_t >
    struct is_small {
        static const bool value = sizeof (local_erasure< source, typename implicit_object_to_parameter< sig >::type ... >) <= sizeof (storage)
            && alignof (source) <= alignof (decltype (storage))
            && ! std::uses_allocator< source, allocator >::value
            && std::is_nothrow_move_constructible< source >::value;
    };
    
    template< typename, typename = typename wrapper::wrapper_base >
    struct is_compatibly_wrapped : std::false_type
        { static const bool with_compatible_allocation = false; };
    template< typename source >
    struct is_compatibly_wrapped< source, typename source::wrapper_base >
        : std::true_type {
        static const bool with_compatible_allocation = allocator_manager::noexcept_move_adopt
            || std::is_same< typename wrapper::allocator_t, typename source::wrapper::allocator_t >::value;
    };
    
    // Adopt by move.
    template< typename source >
    typename std::enable_if< is_compatibly_wrapped< source >::value >::type
    init( in_place_t< source >, source && s ) {
        typename source::wrapper & o = s;
        auto nontrivial = std::get< + dispatch_slot::move_constructor_destructor >( o.erasure().table );
        if ( ! nontrivial ) {
            std::memcpy( & erasure(), & o.erasure(), sizeof (storage) );
        } else {
            nontrivial( std::move( o ).erasure(), & erasure(), o.any_allocator(), allocator_manager::compatible_allocator( o.erasure() ) );
        }
        o.init( in_place_t< std::nullptr_t >{}, nullptr );
    }
    // Adopt by copy.
    template< typename source >
    typename std::enable_if< is_compatibly_wrapped< source >::value >::type
    init( in_place_t< source >, source const & s ) {
        typename wrapper::wrapper_base const & o = s;
        auto nontrivial = std::get< + dispatch_slot::copy_constructor >( o.erasure().table );
        if ( ! nontrivial ) std::memcpy( & erasure(), & o.erasure(), sizeof (storage) );
        else nontrivial( o.erasure(), & erasure(), allocator_manager::compatible_allocator( o.erasure() ) );
    }
    
    // In-place construction of a compatible source uses a temporary to check its constraints.
    template< typename source, typename ... arg >
    typename std::enable_if< is_compatibly_wrapped< source >::value && ! std::is_same< source, wrapper >::value >::type
    init( in_place_t< source > t, arg && ... a )
        { init( t, source( std::forward< arg >( a ) ... ) ); }
    
    // Discard an allocator argument that is unused or already permanently retained.
    template< typename allocator, typename source, typename ... arg >
    typename std::enable_if< is_small< source >::value
        || ( is_compatibly_wrapped< source >::value && std::is_same< allocator, typename allocator_manager::allocator_t >::value ) >::type
    init( std::allocator_arg_t, allocator const &, in_place_t< source > t, arg && ... a )
        { init( t, std::forward< arg >( a ) ... ); }
    
    // Otherwise, given an allocator and constructor arguments for obtaining a compatible source, normalize allocation by introducing a temporary container object.
    template< typename allocator, typename source, typename ... arg >
    typename std::enable_if< is_compatibly_wrapped< source >::value && ! std::is_same< allocator, typename allocator_manager::allocator_t >::value >::type
    init( std::allocator_arg_t, allocator const & alloc, in_place_t< source > t, arg && ... a )
        { init( in_place_t< wrapper< target_policy, wrapper_allocator< allocator >, sig ... > >{}, std::allocator_arg, alloc, t, std::forward< arg >( a ) ... ); }
    
    // Local erasures.
    template< typename source, typename ... arg >
    typename std::enable_if< is_small< source >::value >::type
    init( in_place_t< source >, arg && ... a ) {
        new (storage_address()) local_erasure< source, typename implicit_object_to_parameter< sig >::type ... >
            ( typename target_policy::copies{}, std::forward< arg >( a ) ... );
    }
    
    // Allocated erasures.
    template< typename in_allocator, typename source, typename ... arg >
    typename std::enable_if< ! is_compatibly_wrapped< source >::value && ! is_small< source >::value >::type
    init( std::allocator_arg_t, in_allocator && alloc, in_place_t< source >, arg && ... a ) {
        typedef typename std::allocator_traits< typename std::decay< in_allocator >::type >::template rebind_alloc< source > allocator;
        typedef allocator_erasure< allocator, source, typename implicit_object_to_parameter< sig >::type ... > erasure;
        static_assert ( is_allocator_erasure< erasure >::value, "" );
        // TODO: Add a new erasure template to put the fancy pointer on the heap.
        static_assert ( sizeof (erasure) <= sizeof storage, "Stateful allocator or fancy pointer is too big for polymorphic function wrapper." );
        new (storage_address()) erasure( typename target_policy::copies{}, std::allocator_arg, alloc, std::forward< arg >( a ) ... );
    }
    template< typename source, typename ... arg >
    typename std::enable_if< ! is_compatibly_wrapped< source >::value && ! is_small< source >::value >::type
    init( in_place_t< source > t, arg && ... a )
        { init( std::allocator_arg, allocator_manager::actual_allocator(), t, std::forward< arg >( a ) ... ); }
    
    wrapper & finish_assign ( wrapper && next ) noexcept {
        destroy();
        init( in_place_t< wrapper >{}, std::move( next ) );
        this->actual_allocator() = next.actual_allocator();
        return * this;
    }
    
    void destroy() noexcept {
        auto nontrivial = std::get< + dispatch_slot::destructor >( this->erasure().table );
        if ( nontrivial ) nontrivial( this->erasure(), allocator_manager::any_allocator() );
    }
public:
    using wrapper::wrapper_base::operator ();
    using wrapper::wrapper_base::target;
    using wrapper::wrapper_base::target_type;
    using wrapper::wrapper_base::verify_type;
    using wrapper::wrapper_base::operator bool;
    using wrapper::wrapper_base::complete_object_address;
    
    wrapper() noexcept
        { init( in_place_t< std::nullptr_t >{}, nullptr ); }
    wrapper( wrapper && s ) noexcept
        : allocator_manager( std::move( s ) )
        { init( in_place_t< wrapper >{}, std::move( s ) ); }
    wrapper( wrapper const & s )
        : allocator_manager( s )
        { init( in_place_t< wrapper >{}, s ); }
    
    template< typename allocator >
    wrapper( std::allocator_arg_t, allocator const & alloc )
        : allocator_manager( std::allocator_arg, alloc )
        { init( in_place_t< std::nullptr_t >{}, nullptr ); }
    
    template< typename source,
        typename std::enable_if<
            is_targetable< typename std::decay< source >::type >::value
            && std::is_constructible< typename std::decay< source >::type, source && >::value
            && ! std::is_base_of< wrapper, typename std::decay< source >::type >::value
        >::type * = nullptr >
    wrapper( source && s )
    noexcept( is_noexcept_erasable< typename std::decay< source >::type >::value
            || is_compatibly_wrapped< source >::with_compatible_allocation )
        { init( in_place_t< typename std::decay< source >::type >{}, std::forward< source >( s ) ); }
    
    template< typename allocator, typename source,
        typename = typename std::enable_if<
            is_targetable< typename std::decay< source >::type >::value
        >::type >
    wrapper( std::allocator_arg_t, allocator const & alloc, source && s )
    noexcept( is_noexcept_erasable< typename std::decay< source >::type >::value )
        : allocator_manager( std::allocator_arg, alloc )
        { init( std::allocator_arg, alloc, in_place_t< typename std::decay< source >::type >{}, std::forward< source >( s ) ); }
    
    template< typename source, typename ... arg,
        typename = typename std::enable_if< is_targetable< source >::value >::type >
    wrapper( in_place_t< source > t, arg && ... a )
    noexcept( is_noexcept_erasable< source >::value )
        { init( t, std::forward< arg >( a ) ... ); }
    
    template< typename allocator, typename source, typename ... arg,
        typename = typename std::enable_if<
            is_targetable< source >::value
        >::type >
    wrapper( std::allocator_arg_t, allocator const & alloc, in_place_t< source > t, arg && ... a )
    noexcept( is_noexcept_erasable< source >::value )
        : allocator_manager( std::allocator_arg, alloc )
        { init( std::allocator_arg, alloc, t, std::forward< arg >( a ) ... ); }
    
    ~ wrapper() noexcept
        { destroy(); }
    
    wrapper & operator = ( wrapper && s )
    noexcept ( allocator_manager::noexcept_move_assign ) {
        if ( & s == this ) return * this;
        allocator_manager::operator = ( std::move( s ) );
        return finish_assign( allocator_manager::template reallocate< wrapper >( std::move( s ) ) );
    }
    wrapper & operator = ( wrapper const & s ) {
        if ( & s == this ) return * this;
        allocator_manager::operator = ( s );
        return finish_assign( allocator_manager::template reallocate< wrapper >( s ) );
    }
    
    template< typename source,
        typename std::enable_if<
            is_compatibly_wrapped< typename std::decay< source >::type >::value
            && std::is_constructible< typename std::decay< source >::type, source && >::value
            && ! std::is_base_of< wrapper, typename std::decay< source >::type >::value
        >::type * = nullptr >
    wrapper &
    operator = ( source && s )
    noexcept( is_compatibly_wrapped< source >::with_compatible_allocation && allocator_manager::noexcept_move_assign )
        { return finish_assign( allocator_manager::template reallocate< wrapper >( std::forward< source >( s ) ) ); }
    
    template< typename source,
        typename std::enable_if<
            is_targetable< typename std::decay< source >::type >::value
            && std::is_constructible< typename std::decay< source >::type, source && >::value
            && ! is_compatibly_wrapped< typename std::decay< source >::type >::value
        >::type * = nullptr >
    wrapper &
    operator = ( source && s )
    noexcept( is_noexcept_erasable< typename std::decay< source >::type >::value )
        { return finish_assign( wrapper{ std::allocator_arg, this->actual_allocator(), std::forward< source >( s ) } ); }
    
    template< typename allocator, typename source,
        typename = typename std::enable_if< is_targetable< typename std::decay< source >::type >::value >::type >
    wrapper &
    assign( source && s, allocator const & alloc )
    noexcept( is_noexcept_erasable< typename std::decay< source >::type >::value || is_compatibly_wrapped< source >::value )
        { return finish_assign( wrapper{ std::allocator_arg, alloc, std::forward< source >( s ) } ); }
    
    template< typename source, typename ... arg,
        typename = typename std::enable_if< is_targetable< source >::value >::type >
    wrapper &
    emplace_assign( arg && ... a )
    noexcept( is_noexcept_erasable< source >::value )
        { return finish_assign( wrapper{ std::allocator_arg, this->actual_allocator(), in_place_t< source >{}, std::forward< arg >( a ) ... } ); }
    
    template< typename source, typename allocator, typename ... arg,
        typename = typename std::enable_if< is_targetable< source >::value >::type >
    wrapper &
    allocate_assign( allocator const & alloc, arg && ... a )
    noexcept( is_noexcept_erasable< source >::value ) {
        return finish_assign( wrapper< target_policy, wrapper_allocator< allocator >, sig ... >
            { std::allocator_arg, alloc, in_place_t< source >{}, std::forward< arg >( a ) ... } );
    }
    
    void swap( wrapper & o ) noexcept {
        this->allocator_manager::swap( o );
        /*  Use the two-assignment algorithm. Each target is safely restored to its original allocator.
            Calling std::swap would be sufficient, except that POCMA might not be set. */
        wrapper temp( std::move( * this ) );
        destroy();
        init( in_place_t< wrapper >{}, std::move( o ) );
        o.destroy();
        o.init( in_place_t< wrapper >{}, std::move( temp ) );
    }
};

#undef UNPACK
#undef IGNORE
#undef DISPATCH_CQ
#undef DISPATCH_CV
#undef DISPATCH_CVREFQ
#undef DISPATCH_ALL
#undef DISPATCH_TABLE
}

// The actual classes all just pull their interfaces out of private inheritance.
template< typename ... sig >
class function
    : impl::wrapper< impl::is_copyable_all_callable< function< sig ... >, sig ... >, impl::wrapper_no_allocator, sig ... > {
    template< typename, typename, typename ... >
    friend class impl::wrapper;
public:
    using function::wrapper::wrapper;
    
    function() noexcept = default; // Investigate why these are needed. Compiler bug?
    function( function && s ) noexcept = default;
    function( function const & ) = default;
    function & operator = ( function && o ) noexcept OLD_GCC_SKIP( = default; )
        OLD_GCC_FIX ( { function::wrapper::operator = ( static_cast< typename function::wrapper && >( o ) ); return * this; } )
    function & operator = ( function const & o ) = default;
    
    using function::wrapper::operator ();
    using function::wrapper::operator =;
    using function::wrapper::swap;
    using function::wrapper::target;
    using function::wrapper::target_type;
    using function::wrapper::verify_type;
    using function::wrapper::operator bool;
    using function::wrapper::complete_object_address;
    
    using function::wrapper::assign;
    using function::wrapper::emplace_assign;
    using function::wrapper::allocate_assign;
    // No allocator_type or get_allocator.
};

template< typename ... sig >
class unique_function
    : impl::wrapper< impl::is_all_callable< sig ... >, impl::wrapper_no_allocator, sig ... > {
    template< typename, typename, typename ... >
    friend class impl::wrapper;
public:
    using unique_function::wrapper::wrapper;
    
    unique_function() noexcept = default;
    unique_function( unique_function && s ) noexcept = default;
    unique_function( unique_function const & ) = delete;
    unique_function & operator = ( unique_function && o ) noexcept OLD_GCC_SKIP( = default; )
        OLD_GCC_FIX ( { unique_function::wrapper::operator = ( static_cast< typename unique_function::wrapper && >( o ) ); return * this; } )
    unique_function & operator = ( unique_function const & o ) = delete;
    
    using unique_function::wrapper::operator ();
    using unique_function::wrapper::operator =;
    using unique_function::wrapper::swap;
    using unique_function::wrapper::target;
    using unique_function::wrapper::target_type;
    using unique_function::wrapper::verify_type;
    using unique_function::wrapper::operator bool;
    using unique_function::wrapper::complete_object_address;
    
    using unique_function::wrapper::assign;
    using unique_function::wrapper::emplace_assign;
    using unique_function::wrapper::allocate_assign;
    // No allocator_type or get_allocator.
};

template< typename allocator, typename ... sig >
class function_container
    : impl::wrapper< impl::is_copyable_all_callable< function_container< allocator, sig ... >, sig ... >, impl::wrapper_allocator< allocator >, sig ... > {
    template< typename, typename, typename ... >
    friend class impl::wrapper;
public:
    using function_container::wrapper::wrapper;
    
    function_container() noexcept = default; // Investigate why these are needed. Compiler bug?
    function_container( function_container && s ) noexcept = default;
    function_container( function_container const & ) = default;
    function_container & operator = ( function_container && s ) = default;
    function_container & operator = ( function_container const & o ) = default;
    
    using function_container::wrapper::operator ();
    using function_container::wrapper::operator =;
    using function_container::wrapper::swap;
    using function_container::wrapper::target;
    using function_container::wrapper::target_type;
    using function_container::wrapper::verify_type;
    using function_container::wrapper::operator bool;
    using function_container::wrapper::complete_object_address;
    
    typedef typename function_container::wrapper::allocator_t allocator_type;
    using function_container::wrapper::get_allocator;
    using function_container::wrapper::emplace_assign;
    // No assign or allocate_assign.
};

template< typename allocator, typename ... sig >
class unique_function_container
    : impl::wrapper< impl::is_all_callable< sig ... >, impl::wrapper_allocator< allocator >, sig ... > {
    template< typename, typename, typename ... >
    friend class impl::wrapper;
public:
    using unique_function_container::wrapper::wrapper;
    
    unique_function_container() noexcept = default;
    unique_function_container( unique_function_container && s ) noexcept = default;
    unique_function_container( unique_function_container const & ) = delete;
    unique_function_container & operator = ( unique_function_container && s ) = default;
    unique_function_container & operator = ( unique_function_container const & o ) = delete;
    
    using unique_function_container::wrapper::operator ();
    using unique_function_container::wrapper::operator =;
    using unique_function_container::wrapper::swap;
    using unique_function_container::wrapper::target;
    using unique_function_container::wrapper::target_type;
    using unique_function_container::wrapper::verify_type;
    using unique_function_container::wrapper::operator bool;
    using unique_function_container::wrapper::complete_object_address;
    
    typedef typename unique_function_container::wrapper::allocator_t allocator_type;
    using unique_function_container::wrapper::get_allocator;
    using unique_function_container::wrapper::emplace_assign;
    // No assign or allocate_assign.
};

#define DEFINE_WRAPPER_OPS( NAME ) \
template< typename ... sig > \
bool operator == ( NAME< sig ... > const & a, std::nullptr_t ) \
    { return !a; } \
template< typename ... sig > \
bool operator != ( NAME< sig ... > const & a, std::nullptr_t ) \
    { return a; } \
template< typename ... sig > \
bool operator == ( std::nullptr_t, NAME< sig ... > const & a ) \
    { return !a; } \
template< typename ... sig > \
bool operator != ( std::nullptr_t, NAME< sig ... > const & a ) \
    { return a; } \
template< typename ... sig > \
void swap( NAME< sig ... > & lhs, NAME< sig ... > & rhs ) \
    noexcept(noexcept( lhs.swap( rhs ) )) \
    { lhs.swap( rhs ); }

DEFINE_WRAPPER_OPS( function )
DEFINE_WRAPPER_OPS( unique_function )
DEFINE_WRAPPER_OPS( function_container )
DEFINE_WRAPPER_OPS( unique_function_container )

#undef DEFINE_WRAPPER_OPS

#if __cplusplus >= 201402 // Return type deduction really simplifies these.
// See proposal, "std::recover: undoing type erasure"

template< typename erasure >
void const volatile * recover_address( erasure & e, std::false_type )
    { return e.complete_object_address(); }

template< typename erasure >
void const volatile * recover_address( erasure & e, std::true_type )
    { return e.referent_address(); }

struct bad_type_recovery : std::exception
    { virtual char const * what() const noexcept override { return "An object was not found with its expected type."; } };

template< typename want, typename erasure_ref >
constexpr auto && recover( erasure_ref && e ) {
    typedef std::remove_reference_t< erasure_ref > erasure;
    typedef std::conditional_t< std::is_const< erasure >::value, want const, want > prop_const;
    typedef std::conditional_t< std::is_volatile< erasure >::value, prop_const volatile, prop_const > prop_cv;
    typedef std::conditional_t< std::is_lvalue_reference< erasure_ref >::value, prop_cv &, prop_cv && > prop_cvref;
    if ( e.template verify_type< want >() ) {
        return static_cast< prop_cvref >( * ( std::decay_t< want > * ) recover_address( e, std::is_reference< want >{} ) );
    } else throw bad_type_recovery{};
}
#endif

#undef is_trivially_move_constructible
#undef is_trivially_copy_constructible
#undef deprecated
}

#endif
