
# Empty cluster.   Request a CephFS filesystem using 100% of the cluster


# Request a large number of pools with a target size that adds up
# to more than the actual capacity of the crush root.

# 1. Request a number of pools with no target size or ratio, then put a lot
# of data into one of them, see its pg count get sized up properly.
# 2. Drain half the data from the first pool, and write that amount
#    again to another pool: should end up with two pools with the same
#    pg num (depending on how we threshold the adjustments...)


# Creating pools that use a rool which starts somewhere other than
# the overall root, but an existing rule at a higher level is placing
# some of its PGs into our subtree.



# If I ask for a 1GB CephFS poolset, and the CephFS definition says
# that data, metadata pools should be weight 1.0, 0.1, then
# if they both map to the hdd pool they should



# Create 10 pools.  Fill them all 51% so that none of them is elegible
# for shrinking.