 Using Golang and the AWS Golang SDK to upload objects to a bucket against a vstart cluster.
======
## Requirements
1. A Linux based environment with a minimum of 8 CPU/VCPU, 16G RAM and 100GB disk. 
   + Recommended distro: 
   1. Fedora - (34 or higher)
   2. Ubuntu (20.04 and up)
   3. OpenSuse (Leap 15.2 or tumbleweed)
 It is not recommended to use WSL because the build time takes longer than running native Linux.

## Steps to build
1. Clone the Ceph repository from [here](https://github.com/ceph/ceph).
2. Follow [this](https://github.com/ceph/ceph/blob/master/README.md) READ ME on building Ceph.
3. Run unit [tests](https://github.com/ceph/ceph#running-unit-tests).
4. Run the ceph process by running `cd build` followed by the following command
``` MON=1 OSD=1 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n -d ```
5. Since the create bucket command does not yet work properly in my codebase, I decided to use the s3cmd tool to create a bucket.
 Run the command
 
```
s3cmd --no-ssl --host=localhost:8000 --host-bucket="localhost:8000/%(bucket)" \
--access_key=0555b35654ad1656d804 \
--secret_key=h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q== \
mb s3://cephbucket
```
6. Then cd into the Golang folder and run `go build` then `./ceph`.
7. Your result should look like this 
![Golang result](https://i.ibb.co/mtNMRTZ/golang.png)
PS; The aws config credentials I used are:
    

I hope you find this useful.

