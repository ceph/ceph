#############################################################################################################################
#
# opens all failed(or dead) jobs on teuthology server using vim
# usage: python3 log_to_vim.py $job_name
# example: python3 log_to_vim.py yuriw-2021-01-06_21:20:32-rados-wip-yuri3-testing-2021-01-06-0820-octopus-distro-basic-smithi
#
##############################################################################################################################
#!/usr/bin/env python3
import re
import os
import sys

if (len(sys.argv) == 2):
	run_name = str(sys.argv[1])
	archive = "/ceph/teuthology-archive/" + run_name + "/"
	print(" opening failed/dead jobs for " + archive)
	print(".......")

	scrape = archive + "scrape.log"

	with open(scrape,'r') as f:
		job_id = re.findall(r"\'\b[0-9]{7}\b\'", f.read())
		print("jobs that failed...")

	for idx, item in enumerate(job_id):
		job_id[idx] = archive + item.strip("'")+ "/teuthology.log"
		print("job id" + job_id[idx])

	os.system("vim -p " +" ".join(job_id))
else:
	print("please enter run name")
	print("usage: python3 log_to_vim.py $job_name")

