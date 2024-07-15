原文地址：[Linux内核设计与实现(7)---内核数据结构](http://chinaunix.net/uid-24708340-id-3854894.html) 作者：[leon_yu](http://chinaunix.net/uid/24708340.html)

Linux内核实现了一些通用的数据结构，提倡大家在开发时重用，内核开发者应该尽可能地使用这些数据结构。最常用的有：链表、队列、映射、二叉树 

1.链表

(1).链表是一种存放和操作可变数量元素(节点)的数据结构，动态创建，无需在内存中占用连续内存区。每个元素都必须包含指向下一个元素的指针，当有元素加入或从链表中删除时，只需要调整相应指针即可。

单向链表，每个节点指向一个后驱  

```
1.  /* an element in a linked list */ 
2.  struct list_element {

3.  void *data; /* the payload */

4.  struct list_element *next; /* pointer to the next element */

5.  };
```

![image](https://upload-images.jianshu.io/upload_images/2099201-8865757d2615f6a8.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

双向链表：每个节点分别有一个指向后驱和一个指向前驱的指针

```
1.  /* an element in a linked list */ 
2.  struct list_element {

3.  void *data; /* the payload */

4.  struct list_element *next; /* pointer to the next element */

5.  struct list_element *prev; /* pointer to the previous element */

6.  };
```

![image](https://upload-images.jianshu.io/upload_images/2099201-0a9a61791a1e87ab.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

环形链表：将最后一个节点的next指向头节点，头节点的prev指向最后一个节点
![image.png](https://upload-images.jianshu.io/upload_images/2099201-eb27d30066964ce8.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


因为双向环形链表提供了最大的灵活性，所以Linux内核的标准链表采用环形双向链表实现的。

沿链表移动

沿链表移动只能是线性移动，先访问某个元素，然后向后访问下一个元素，不断如此重复后移；如果需要随机访问，一般不用链表。

使用链表的理想情况是：需要遍历所有数据或需要动态加入或删除数据时。   

(2) Linux内核中链表的实现

①Linux内核实现方式独树一帜，它不是将数据结构塞入链表，而是将链表节点塞入数据结构；

链表结构在中声明
```
struct list_head {
    struct list_head *next, *prev;
};
```
比如创建一个fox数据结构来描述犬科动物一员，存储链表节点到fox数据结构中

```
1.  struct fox { 
2.  unsigned long tail_length; /* length in centimeters of tail */

3.  unsigned long weight; /* weight in kilograms */

4.  bool is_fantastic; /* is this fox fantastic? */

5.  struct list_head list; /* list of all fox structures */

6.  };
```

Fox中的list.next指向下一个元素，list.prev指向前一个元素，这样链表就可以被内核使用了。可以用一系列list_add(),list_del()等来操作，但他们有个共同特点，就是只接受list_head结构参数。

这样内核就用一个统一的链表实现，可以操作各种数据结构的应用；使用container_of宏可以很方便的从链表指针找到父结构中任何成员变量；

```
1.  #define container_of(ptr, type, member) ({            \ 
2.  const typeof(((type *)0)->member) * __mptr = (ptr);    \

3.  (type *)((char *)__mptr - offsetof(type, member)); })

4.  #endif
```

在C语言中，一个给定结构中的变量偏移，在编译时地址就被ABI固定下来了。

使用container_of()宏，定义一个简单的函数可以方便的返回包含list_head的父类结构体；

```
1.  /** 
2.  * list_entry - get the struct for this entry

3.  * @ptr:    the &struct list_head pointer.

4.  * @type:    the type of the struct this is embedded in.

5.  * @member:    the name of the list_struct within the struct.

6.  */

7.  #define list_entry(ptr, type, member) \

8.  container_of(ptr, type, member)
```

依靠list_entry()方法，内核提供了创建，操作以及其他管理链表的各种例程—所有这些方法都不需要知道list_head所嵌入对象的数据结构。

②定义一个链表

list_head本身并没有意义，它需要被嵌入到你自己的数据结构中才能生效。

```
1.  struct fox { 
2.  unsigned long tail_length; /* length in centimeters of tail */

3.  unsigned long weight; /* weight in kilograms */

4.  bool is_fantastic; /* is this fox fantastic? */

5.  struct list_head list; /* list of all fox structures */

6.  };
```
链表使用前需要初始化：

运行时动态初始化链表

```
1.  struct fox *red_fox; 
2.  red_fox = kmalloc(sizeof(*red_fox), GFP_KERNEL);

3.  red_fox->tail_length = 40;

4.  red_fox->weight = 6;

5.  red_fox->is_fantastic = false;

6.  INIT_LIST_HEAD(&red_fox->list);
```

如果一个结构在编译期静态创建，而你需要在其中一个链表中直接引用，下面方式最简洁：

```
1.  struct fox red_fox = { 
2.  .tail_length = 40,

3.  .weight = 6,

4.  .list = LIST_HEAD_INIT(red_fox.list),

5.  };
```

如果需要明确的链表头，也可以这样声明

static LIST_HEAD(fox_list); 

(3)链表基本操作(算法复杂度全都是O(1))

static inline void list_add(struct list_head *new, struct list_head *head);

list_add向链表头后面添加新项，可以用来实现一个栈; 

假定创建一个新的struct fox，并把它加入fox_list，那么

list_add(&f->list,&fox_list) 

static inline void list_add_tail(struct list_head *new, struct list_head *head);

list_add_tail向链表头的前面添加节点，可以实现一个FIFO； 

static inline void list_del(struct list_head *entry);

例如删除上面添加的f, list_del(&f->list);

如果entry删除之后，还有可能添加到另一个链表，删除之后，还应做初始化，用

static inline void list_del_init(struct list_head *entry); 

static inline void list_move(struct list_head *list, struct list_head *head);

把节点list从链表中移除，然后加入另一个链表head节点的后面； 

static inline void list_move_tail(struct list_head *list, struct list_head *head);

把节点list从链表中移除，然后加入另一个链表head节点的前面； 

static inline int list_empty(const struct list_head *head);检查链表是否为空 

static inline void list_splice(const struct list_head *list, struct list_head *head) 

把链表list添加到head的后面去 

还有其他更多操作参见

(4)遍历链表:算法复杂度是O(n)

```

1.  list_for_each_entry(pos, head, member); 
2.  struct fox *f;

3.  list_for_each_entry(f, &fox_list, list) {

4.  /* on each iteration, ‘f’ points to the next fox structure ... */

5.  if (f->is_fantastic) return f;

6.  }
```

上面宏中f返回每次获取的结构体，fox_list是链表头, list是链表头在fox结构体里的成员名字

list_for_each_entry_reverse(pos, head, member)；//反向遍历

list_for_each_entry_safe(pos, n, head, member)；//遍历可以删除pos，n与pos同类型

注意：当用list_for_each_entry()的安全版本遍历链表时，有可能会有其他地方并发操作删除节点，所以做删除操作时，必须锁定链表。 

2.队列

任何操作系统内核都少不了一种编程模型：生产者和消费者。生产者产生数据，消费者处理数据。实现该模型，最简单的就是队列。生产者将数据入队列，消费者摘取数据。

![image.png](https://upload-images.jianshu.io/upload_images/2099201-c76f96f6f8b2a68a.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


Linux内核通用队列实现为kfifo，在声明：

(1)创建队列

int kfifo_alloc(struct kfifo *fifo, unsigned int size, gfp_t gfp_mask);//成功返回0

void kfifo_init(struct kfifo *fifo, void *buffer, unsigned int size);//用buffer指向的内存来实现fifo

静态声明

DECLARE_KFIFO(name,size);

INIT_KFIFO(name); 

 (2)操作队列

入列 ：unsigned int kfifo_in(struct kfifo *fifo, const void *from, unsigned int len)；

出列 ：unsigned int kfifo_out(struct kfifo *fifo, void *to, unsigned int len)；

出列的数据，就不存在于fifo中了，要是只“偷窥”队列中数据，而不想删除它，可以用

unsigned int kfifo_out_peek(struct kfifo *fifo, void *to, unsigned int len, unsigned offset)；

(3)其他操作

获取kfifo总体大小

static inline __must_check unsigned int kfifo_size(struct kfifo *fifo)； 

获取Kfifo中已推入数据长度

static inline unsigned int kfifo_len(struct kfifo *fifo)； 

获取Kfifo中海油多少空间可用

static inline __must_check unsigned int kfifo_avail(struct kfifo *fifo)； 

判断kfifo为空或满

static inline __must_check int kfifo_is_empty(struct kfifo *fifo)；

static inline __must_check int kfifo_is_full(struct kfifo *fifo)； 

销毁队列

void kfifo_free(struct kfifo *fifo); 

重置队列

static inline void kfifo_reset(struct kfifo *fifo)；//抛弃所有队列内容

3.映射

虽然散列表是一种映射，但并非所有映射都是散列表实现的。此外，还可以用自平衡二叉搜索树存储数据。散列表提供更好的平均的渐进复杂度，但二叉搜索树在最坏的情况下能有更好的表现(对数复杂性相比线性复杂性)，散列表同时满足顺序保证。

一个映射至少实现三种操作

*add (key, value)

*remove(key)

*value = lookup(key)

Linux内核提供了一个简单有效的映射数据结构，但并非是通用的映射，它的目标是：映射一个唯一的标识数(UID)到一个指针。 

(1)初始化一个idr

void idr_init(struct idr *idp);

For example:

struct idr id_huh;  /* statically define idr structure */

idr_init(&id_huh);  /* initialize provided idr structure */ (2) 

(2)分配一个新的UID，分两步

①调整后备树大小方法

int idr_pre_get(struct idr *idp, gfp_t gfp_mask)；//成功返回1

②获取新的UID，并将其添加到idr的方法

int idr_get_new(struct idr *idp, void *ptr, int *id)；//分配新UID关联到ptr上，UID存于id

```

1.  Let’s look at a full example: 
2.  int id;

3.  do {

4.  if (!idr_pre_get(&idr_huh, GFP_KERNEL))

5.  return -ENOSPC;

6.  ret = idr_get_new(&idr_huh, ptr, &id);

7.  } while (ret == -EAGAIN);
```

如果成功，上述代码将获得一个新的UID，它被存储在变量id中，并且将UID映射到ptr。

int idr_get_new_above(struct idr *idp, void *ptr, int starting_id, int *id)；//可以指定一个最小UID，这样除了新的UID大雨或等于starting_id外，还确保UID系统运行期间唯一 

(3)查找UID

void *idr_find(struct idr *idp, int id)；//调用成功返回ptr指针，失败返回NULL，所以最好不要将UID映射到空指针，这样无法区分调用成功还是失败

```

1.  Usage is simple: 
2.  struct my_struct *ptr = idr_find(&idr_huh, id);

3.  if (!ptr)

4.  return -EINVAL; /* error */
```

(4)删除UID

void idr_remove(struct idr *idp, int id)；//id关联指针从映射中删除 

(5)撤销idr

void idr_destroy(struct idr *idp)；//释放idr中未使用内存，通常内核不会撤销idr，除非关闭或卸载。若需要强制删除所有UID

void idr_remove_all(struct idr *idp)；

应该先调用idr_remove_all(),然后调用idr_destroy()，这样就能是idr占用的内存都被释放。

4.二叉树

树，是一个能提供分层的树形数据结构的特定数据结构。在数学意义上，树是一个无环的、链接的有向图。任何一个节点有0或1个入边，0或多个出边。

二叉树，是指每个节点最多只有2个出边的树。

(1)二叉搜索树(binary search trees)，是一个节点有序的二叉树，其遵循下列法则：

*根的左分支节点值都小于根节点值

*有分支节点值都大于根节点值

*所有的子树也都是二叉搜索树

所以，在BST中搜索一个给定值或按序遍历都相当快捷(对数和线性)。 

(2)平衡二叉搜索树树：所有叶子节点深度差不超过1的二叉搜索树。

自平衡二叉搜索树：指其操作都试图维持(半)平衡的二叉搜索树。

*红黑树，是一种自平衡二叉搜索树，linux主要的平衡二叉树数据结构就是红黑树。红黑树具有特殊的着色属性，遵循下面六个属性，能维持半平衡结构：

①所有叶子节点要么着红色，要么着黑色。

②叶子节点都是黑色。

③叶子节点不包含数据。

④所有非叶子节点都有两个字节点。

⑤如果一个节点是红色，则它的子节点都是黑色。

⑥在一个节点到其叶子节点的路径中，如果总是包含同样数目的黑色节点，则该路径相比其他路径是最短的。

上述条件保证了最深叶子节点的深度不会大于两倍的最浅叶子节点的深度，所以红黑树总是半平衡的。 

Linux实现的红黑树称为rbtree,定义在中。

①rbtree的根节点由数据结构rb_root描述，创建一个红黑树，要分配一个新的rb_root结构，并且需要初始化为特殊值RB_ROOT.

struct rb_root root = RB_ROOT;

树里其他节点由结构rb_node描述。

rbtree的实现并没有提供搜索和插入例程，这些例程由rbtree的用户自己定义，可以使用rbtree提供的辅助函数，但要自己实现比较操作 

②搜索

实例，实现了在页高速缓存中搜索一个文件区(由一个i节点和一个偏移量共同描述)，每个i节点都有自己的rbtree，以关联在文件中的页便宜。下面函数搜索给定i节点的rbtree，以寻找匹配的偏移值

```

1.  struct page * rb_search_page_cache(struct inode *inode, unsigned long offset) 
2.  {

3.  struct rb_node *n = inode->i_rb_page_cache.rb_node;

5.  while (n) {

6.  struct page *page = rb_entry(n, struct page, rb_page_cache);

7.  if (offset < page->offset)

8.  n = n->rb_left;

9.  else if (offset > page->offset)

10.  n = n->rb_right;

11.  else

12.  return page;

13.  }

14.  return NULL;

15.  }
```

在while循环中遍历了整个rbtree, offset做比较操作，循环中找到一个匹配的offset节点，则搜索完成，返回page结构，遍历完全树都没找到，则返回NULL。 

③插入操作，必须实现搜索和插入逻辑

```

1.  struct page * rb_insert_page_cache(struct inode *inode, unsigned long offset, struct rb_node *node) 
2.  {

3.  struct rb_node **p = &inode->i_rb_page_cache.rb_node;

4.  struct rb_node *parent = NULL;

5.  struct page *page;

7.  while (*p) {

8.  parent = *p;

9.  page = rb_entry(parent, struct page, rb_page_cache);

10.  if (offset < page->offset)

11.  p = &(*p)->rb_left;

12.  else if (offset > page->offset)

13.  p = &(*p)->rb_right;

14.  else

15.  return page;

16.  }

17.  rb_link_node(node, parent, p);

18.  rb_insert_color(node, &inode->i_rb_page_cache);

19.  return NULL;

20.  }
```

和搜索不同的是，该函数希望找不到匹配的offset，这样就可以插入叶子节点。找到插入点后，调用rb_link_node(在给定位置插入新节点)，然后调用rb_insert_color()方法执行复杂的再平衡动作。如果页被加入到高速缓存中，则返回NULL。如果页原本已经存在高速缓存中，则返回这个已存在的页结构地址。 

5.数据结构的选择

链表：对数据集合的主要操作是遍历数据，就用链表；当需要存储相对较少的数据项，或当你需要和内核中其他使用链表的代码交互时，首选链表。如果存储的大小不明的数据集合，链表更合适，可以动态添加任何数据类型。 

队列：如果代码符合生产者/消费者模式，就用队列；如果你想用一个定长缓冲，队列的添加和删除操作简单有效；

映射：如果需要映射一个UID到一个对象，就用映射。Linux的映射接口是针对UID到指针的映射，并不适合其他场景。 

红黑树：如果需要存储大量数据，并且迅速检索，用红黑树最好；但如果没有执行太多次时间紧迫的查找操作，则红黑树不是最好选择，可以用链表；

当上述数据结构都不能满足你需要，内核还是下了一些较少使用的数据结构，比如基树和位图，只有当寻遍所有内核提供的数据结构都不能满足时，才需要自己设计数据结构。

经常在独立的源文件中实现的一种常见数据结构是散列表，因为散列表无非是一些“桶”和一个散列函数，而且这个散列函数是针对每个用例的，因此非泛型编程语言实现内核范围内的统一散列表，其实并没有什么价值。 

6.算法复杂度

算法复杂度最常用的技术还是研究算法的渐进行为，渐进行为是指当算法的输入变大非常大或接近于无限大时算法的行为。研究算法的伸缩度(当输入增大时算法执行的变化)可以帮助我们以特定基准抽象出算法模型，从而更好理解算法行为。

(1)大O符号

**大****O****符号（****Big O notation****）**是用于描述[函数](http://zh.wikipedia.org/wiki/%E5%87%BD%E6%95%B0 "函数")[渐近行为](http://zh.wikipedia.org/wiki/%E6%B8%90%E8%BF%91%E5%88%86%E6%9E%90 "渐近分析")的[数学](http://zh.wikipedia.org/wiki/%E6%95%B0%E5%AD%A6 "数学")符号。更确切地说，它是用另一个（通常更简单的）函数来描述一个函数[数量级](http://zh.wikipedia.org/wiki/%E6%95%B0%E9%87%8F%E7%BA%A7 "数量级")的**渐近上界**。在[数学](http://zh.wikipedia.org/wiki/%E6%95%B0%E5%AD%A6 "数学")中，它一般用来刻画被截断的[无穷级数](http://zh.wikipedia.org/wiki/%E6%97%A0%E7%A9%B7%E7%BA%A7%E6%95%B0 "无穷级数")尤其是[渐近级数](http://zh.wikipedia.org/w/index.php?title=%E6%B8%90%E8%BF%91%E7%BA%A7%E6%95%B0&action=edit&redlink=1 "渐近级数（页面不存在）")的剩余项；在[计算机科学](http://zh.wikipedia.org/wiki/%E8%AE%A1%E7%AE%97%E6%9C%BA%E7%A7%91%E5%AD%A6 "计算机科学")中，它在[分析](http://zh.wikipedia.org/wiki/%E7%AE%97%E6%B3%95%E5%88%86%E6%9E%90 "算法分析")[算法](http://zh.wikipedia.org/wiki/%E7%AE%97%E6%B3%95 "算法")[复杂性](http://zh.wikipedia.org/wiki/%E8%A8%88%E7%AE%97%E8%A4%87%E9%9B%9C%E6%80%A7%E7%90%86%E8%AB%96 "计算复杂性理论")的方面非常有用。

(2) 大θ符号

人们讨论的大O符号，实际上更接近于Knuth教授提出的大θ符号，大θ符号更多的是指最小上限，或一个抽象出具有上限和下限的函数。

(3)时间复杂度，常用的函数阶

下面是在分析算法的时候常见的函数分类列表。所有这些函数都处于 [图片上传失败...(image-ad8281-1597288056838)]  趋近于无穷大的情况下，增长得慢的函数列在上面。 [图片上传失败...(image-567b46-1597288056838)]  是一个任意常数。

![image.png](https://upload-images.jianshu.io/upload_images/2099201-dc7e5ac6a09080d3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


显然要避免使用O(n!) or O(2<sup>n</sup>)的算法，比较算法时，还需要考虑输入规模。

我们不赞成使用复杂的算法，但是可要注意算法的负载和典型输入集合大小的关系，不要为了你根本不需要支持的伸缩度要求，盲目地去优化算法。
