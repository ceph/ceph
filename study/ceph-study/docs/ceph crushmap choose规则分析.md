# 1. Placement Rules
## 1.1 模拟代码
```shell
tack(a)
choose
    choose firstn {num} type {bucket-type}
    chooseleaf firstn {num} type {bucket-type}
        if {num} == 0, choose pool-num-replicas buckets (all available).
        if {num} > 0 && < pool-num-replicas, choose that many buckets.
        if {num} < 0, it means pool-num-replicas - {num}.
emit
```

## 1.2 Placement Rules的执行流程
1. take操作选择一个bucket, 一般是root类型的bucket.
2. choose操作有不同的选择方式，其输入都是上一步的输出：
    a. choose firstn深度优先选择出num个类型为bucket-type个的子bucket.
    b. chooseleaf先选择出num个类型为bucket-type个子bucket,然后递归到叶节点，选择一个OSD设备：
      - 如果num为0， num就为pool设置的副本数。
      - 如果num大于0， 小于pool的副本数，那么久选择出num个。
      - 如果num小于0，就选择出pool的副本数减去num的绝对值。
3. emit输出结果

# 2. 实战模拟演练
## 2.1 演练列表
ruleset_id | choose num |  chooseleaf_num | 结论 |
---|---|---|---|
0 | firstn 0 type pod | firstn 0 type rack | pg三个副本分布: <br/> - 同一个pod下 <br/> - 不同rack下 |
1 | firstn 1 type pod | firstn 0 type rack | pg三个副本分布: <br/> - 同一个pod下 <br/> - 不同rack下 |
2 | firstn 2 type pod | firstn 0 type rack | pg三个副本分布: <br/> - 同一个pod下 <br/> - 不同rack下 |
3 | firstn 3 type pod | firstn 0 type rack | pg三个副本分布: <br/> - 同一个pod下 <br/> - 不同rack下 |
4 | firstn 4 type pod | firstn 0 type rack | pg三个副本分布: <br/> - 同一个pod下 <br/> - 不同rack下 |
5 | firstn 1 type pod | firstn 1 type rack | pg三个副本分布: <br/> - 同一个pod下 <br/> - 不同rack下 |
6 | firstn 1 type pod | firstn 2 type rack | pg三个副本分布: <br/> - 同一个pod下 <br/> - 不同rack下 |
7 | firstn 1 type pod | firstn 3 type rack | pg三个副本分布: <br/> - 同一个pod下 <br/> - 不同rack下 |
8 | firstn 1 type pod | firstn 4 type rack | pg三个副本分布: <br/> - 同一个pod下 <br/> - 不同rack下 |
9 |  | firstn 0 type pod | pg三个副本分布: <br/> - 不同pod下 |
10 |  | firstn 0 type rack | pg三个副本分布: <br/> - 不同rack下 |
