
Extent 能有效地减少元数据开销。为了进一步理解这个问题，我们还是看看 ext2 中的反面例子。

       ext2/3 以 block 为基本单位，将磁盘划分为多个 block 。为了管理磁盘空间，文件系统需要知道哪些 block 是空闲的。 Ext 使用 bitmap 来达到这个目的。 Bitmap 中的每一个 bit 对应磁盘上的一个 block，当相应 block 被分配后，bitmap 中的相应 bit 被设置为 1 。这是很经典也很清晰的一个设计，但不幸的是当磁盘容量变大时，bitmap 自身所占用的空间也将变大。这就导致了扩展性问题，随着存储设备容量的增加，bitmap 这个元数据所占用的空间也随之增加。而人们希望无论磁盘容量如何增加，元数据不应该随之线形增加，这样的设计才具有可扩展性。

       下图比较了 block 和 extent 的区别：**采用 extent 的 btrfs 和采用 bitmap 的 ext2/3**

![采用extent的btrfs和采用bitmap的ext2/3](https://upload-images.jianshu.io/upload_images/2099201-8de88a4c5684975e.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

       在 ext2/3 中，10 个 block 需要 10 个 bit 来表示；在 btrfs 中则只需要一个元数据。对于大文件，extent 表现出了更加优异的管理性能。

       Extent 是 btrfs 管理磁盘空间的最小单位，由 extent tree 管理。 Btrfs 分配 data 或 metadata 都需要查询 extent tree 以便获得空闲空间的信息。

       注：EXT4文件系统也采用了基于extend的文件存储


**extent 和 bitmap**
extent 的定义为一段连续的空间，这段连续的空间由 offset/bytes 来描述，没有更细分的粒度；bitmap 则通过 bit 来描述一个 ctl->unit 大小的单元，最小的粒度是 unit，对于 bg 来说是 sectorsize 大小。

从一个 extent 中分配空间的时候，entry 的 offset 起始地址后移，bytes 减少，即从 entry 前面分配；extent 表项如果描述的范围临接，则可以合并，比如，对一个大的 extent 经过多次分配、释放后，会出现多个 extent 表项，每个表项只能描述一段连续区域，如果相邻则可以将这些表项合并。

对于 bitmap 的 free space，每次分配的时候，将 offset 开始对应的 bits 清 0（0 表示分配，1 表示空闲）；由于 bitmap 有 sectorsize 大小的粒度，所以整个范围内的空闲空间可能是破碎的，即可能有比较大的总空闲空间，但是这些空间并不连续。搜索位图，按照两个 0 bit 之间的空间来看连续空闲空间是否可以满足分配的需求。对于 bitmap 表项来说，bytes 并不表示 offset -> offset + bytes - 1 这部分连续空间空闲，只是说明从 offset 起在位图表示的范围内能够找到总共 bytes 大小的空闲空间。

.bytes 表示当前范围中的空闲空间总大小，为 0 表示已经没有空闲空间了，对应的表项可以释放掉了。

extent/bitmap 空闲空间表项位于同一个 rbtree 中，它们可以有相同的起始地址，解决冲突的方式是将 extent 表项放在 bitmap表项之前：
```
* bitmap entry and extent entry may share same offset,
* in that case, bitmap entry comes after extent entry.
```
这样，如果没有特别的要求，会优先从 extent 表项分配空间：
```
* allocating from an extent is faster than allocating from a bitmap
```
**什么时候使用 bitmap，什么时候使用 extent？**

添加空闲空间记录的时候，对于小的空间，倾向于使用 extent；相对较大的空间 >= sectorsize * BITS_PER_BITMAP，则使用 bitmap 来记录。具体可以参考 use_bitmap 和 __btrfs_add_free_space。至于分配，则是两者都会尝试，以找到满足分配要求的空间，优先 extent 表示的空闲空间。至于 block group 中是先创建的 bitmap，还是 extent 表项，这要看 block group 的大小了；有了大小值，可以从 use_bitmap 推断出来。
```
　　　　sectorsize * BITS_PER_BITMAP >= 512 * 8 * 4096 = 2^24 = 16M
```
对于 btrfs 来说，当前 sectorsize 为 4096，那么一个 bitmap 可以描述的范围是 
```
2^3 * 2^12 * 2^12 = 2^27 = 128M。
```
多个 extent/bitmap 表项可以描述更多的空间。

对于 free space inode 来说，按照 crc area 的大小，以及每个页面对应一个 crc 表项来看，最大页面数量为：
```
　　　　(4096 - sizeof(u64)*2) / sizeof(u32) = (4096 - 16) / 4 = 1024 - 4 = 1020
```
这样计算，一个 free space 文件的大小限制为 4K * 1020；因为每个表项可以描述不同大小范围的空间，所以 bg 的空间大小取值会有比较大的范围。

**它们表示的空间会不会有重叠？**
bitmap 和 extent 表项表示的空间应该没有重合：小的空间，直接由 extent 表项来表示；从 bitmap 大空间中分配出来的小空间，后面会添加到 extent 中去，而此时 bitmap 对应的位为 0 表示已分配。另外，如果有重叠区域，则 struct btrfs_free_space_ctl->free_space 这个值就不准确了


另外，从关于 free space cluster 的 commit 中可以看到
```
    Currently we either want to refill a cluster with
        　　1) All normal extent entries (those without bitmaps)
        　　2) A bitmap entry with enough space   
```
当前，对于一个 cluster 来说，要么有 extent，要么有 bitmap，不可两者共有.
