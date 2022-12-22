from ctypes import *

class tuple(Structure):
    _fields_ = [("base",   c_int),
                ("limit",  c_int),
                ("obj",    c_int),
                ("offset", c_int),
                ("plba",   c_int)]

LSVD_SUPER = 1
LSVD_DATA = 2
LSVD_CKPT = 3
LSVD_MAGIC = 0x4456534c

# these match version 453d93 of objects.cc

class hdr(Structure):
    _pack_ = 1
    _fields_ = [("magic",               c_uint),
                ("version",             c_uint),
                ("vol_uuid",            c_ubyte*16),
                ("type",                c_uint),
                ("seq",                 c_uint),
                ("hdr_sectors",         c_uint),
                ("data_sectors",        c_uint),
                ("crc",                 c_uint)]
sizeof_hdr = sizeof(hdr) # 44

class super_hdr(Structure):
    _pack_ = 1
    _fields_ = [("vol_size",            c_ulong),
                ("ckpts_offset",        c_uint),
                ("ckpts_len",           c_uint),
                ("clones_offset",       c_uint),
                ("clones_len",          c_uint),
                ("snaps_offset",        c_uint),
                ("snaps_len",           c_uint)]
sizeof_super_hdr = sizeof(super_hdr) # 56

# ckpts is array of uint32

class clone_info(Structure):
    _pack_ = 1
    _fields_ = [("vol_uuid",            c_ubyte*16),
                ("last_seq",            c_uint),
                ("name_len",            c_ubyte)]   # 21 bytes
        # followed by 'name_len' bytes of name
sizeof_clone_info = sizeof(clone_info) # 21

class snap(Structure):
    _pack_ = 1
    _fields_ = [("seq",                 c_uint),
                ("name_len",            c_ubyte)]
sizeof_snap = sizeof(snap) # 5

class data_hdr(Structure):
    _pack_ = 1
    _fields_ = [("cache_seq",           c_ulong),
                ("objs_cleaned_offset", c_uint),
                ("objs_cleaned_len",    c_uint),
                ("map_offset",          c_uint),
                ("map_len",             c_uint)]
sizeof_data_hdr = sizeof(data_hdr) # 24

class obj_cleaned(Structure):
    _pack_ = 1
    _fields_ = [("seq",                 c_uint),
                ("was_deleted",         c_uint)]
sizeof_obj_cleaned = sizeof(obj_cleaned) # 8

class data_map(LittleEndianStructure):
    _pack_ = 1
    _fields_ = [("lba",                 c_ulong, 36),
                ("len",                 c_ulong, 28)]
sizeof_data_map = sizeof(data_map) # 8

class ckpt_hdr(Structure):
    _pack_ = 1
    _fields_ = [("cache_seq",           c_ulong),
                ("ckpts_offset",        c_uint),
                ("ckpts_len",           c_uint),
                ("objs_offset",         c_uint),
                ("objs_len",            c_uint),
                ("deletes_offset",      c_uint),
                ("deletes_len",         c_uint),
                ("map_offset",          c_uint),
                ("map_len",             c_uint)]
sizeof_ckpt_hdr = sizeof(ckpt_hdr) # 40

class ckpt_obj(Structure):
    _pack_ = 1
    _fields_ = [("seq",                 c_uint),
                ("hdr_sectors",         c_uint),
                ("data_sectors",        c_uint),
                ("live_sectors",        c_uint)]
sizeof_ckpt_obj = sizeof(ckpt_obj) # 16

class deferred_delete(Structure):
    _pack_ = 1
    _fields_ = [("seq",                 c_uint),
                ("time",                c_uint)]
sizeof_deferred_delete = sizeof(deferred_delete)

class ckpt_mapentry(LittleEndianStructure):
    _pack_ = 1
    _fields_ = [("lba",                 c_ulong, 36),
                ("len",                 c_ulong, 28),
                ("obj",                 c_uint),
                ("offset",              c_uint)]
sizeof_ckpt_mapentry = sizeof(ckpt_mapentry) # 16

class obj_offset(LittleEndianStructure):
    _pack_ = 1
    _fields_ = [("obj", c_ulong, 36),
                ("offset", c_ulong, 28)]
sizeof_obj_offset = sizeof(obj_offset)

class j_extent(LittleEndianStructure):
    _pack_ = 1
    _fields_ = [("lba", c_ulong, 40),
                ("len", c_ulong, 24)]
sizeof_j_extent = sizeof(j_extent)

class j_map_extent(LittleEndianStructure):
    _pack_ = 1
    _fields_ = [("lba",  c_ulong, 40),
                ("len",  c_ulong, 24),
                ("plba", c_ulong)]
sizeof_j_map_extent = sizeof(j_map_extent)

class j_length(Structure):
    _pack_ = 1
    _fields_ = [("page", c_uint),
                ("len", c_uint)]
sizeof_j_length = sizeof(j_length)

LSVD_J_DATA    = 10
LSVD_J_CKPT    = 11
LSVD_J_PAD     = 12
LSVD_J_SUPER   = 13
LSVD_J_W_SUPER = 14
LSVD_J_R_SUPER = 15

class j_hdr(Structure):
    _pack_ = 1
    _fields_ = [("magic",         c_uint),
                ("type",          c_uint),
                ("version",       c_uint),
                ("len",           c_uint),
                ("seq",           c_ulong),
                ("crc32",         c_uint),
                ("extent_offset", c_uint),
                ("extent_len",    c_uint),
                ("prev",          c_uint)]
sizeof_j_hdr = sizeof(j_hdr)

class j_write_super(Structure):
    _pack_ = 1
    _fields_ = [("magic",       c_uint),
                ("type",        c_uint),
                ("version",     c_uint),
                ("clean",       c_uint),
                ("seq",         c_ulong),
                ("meta_base",   c_int),
                ("meta_limit",  c_int),
                ("base",        c_int),
                ("limit",       c_int),
                ("next",        c_int),
                ("map_start",   c_int),
                ("map_blocks",  c_int),
                ("map_entries", c_int),
                ("len_start",   c_int),
                ("len_blocks",  c_int),
                ("len_entries", c_int)]
sizeof_j_write_super = sizeof(j_write_super)

class j_read_super(Structure):
    _pack_ = 1
    _fields_ = [("magic",        c_uint),
                ("type",         c_uint),
                ("version",      c_uint),
                ("unit_size",    c_int),
                ("base",         c_int),
                ("units",        c_int),
                ("map_start",    c_int),
                ("map_blocks",   c_int)]
sizeof_j_read_super = sizeof(j_read_super)

class j_super(Structure):
    _pack_ = 1
    _fields_ = [("magic",        c_uint),
                ("type",         c_uint),
                ("version",      c_uint),
                ("write_super",  c_uint),
                ("read_super",   c_uint),
                ("vol_uuid",     c_ubyte*16)]
sizeof_j_super = sizeof(j_super)

class iovec(Structure):
    _fields_ = [('iov_base', c_void_p),
                ('iov_len', c_size_t)]

iovec_ptr = POINTER(iovec)
