#include "rados/librados.hpp"
#include "radosstriper/libradosstriper.hpp"
#include <iostream>
#include <string>


int main(int argc, char* argv[])
{
  if(argc != 6)
  {
      std::cout <<"Please put in correct params\n"<<
          "Stripe Count:\n"<<
          "Object Size:\n" <<
          "File Name:\n" <<
          "Object Name:\n"
          "Pool Name:"<< std::endl;
      return EXIT_FAILURE;
  }
  uint32_t strip_count = std::stoi(argv[1]);
  uint32_t obj_size = std::stoi(argv[2]);
  std::string fname = argv[3];
  std::string obj_name = argv[4];
  std::string pool_name = argv[5];
  int ret = 0;
  librados::IoCtx io_ctx;
  librados::Rados cluster;
  libradosstriper::RadosStriper* rs = new libradosstriper::RadosStriper;

  // make sure the keyring file is in /etc/ceph/ and is world readable
  ret = cluster.init2("client.admin","ceph",0);
  if( ret < 0)
  {
      std::cerr << "Couldn't init cluster "<< ret << std::endl;
  }

  // make sure ceph.conf is in /etc/ceph/ and is world readable
  ret = cluster.conf_read_file("ceph.conf");
  if( ret < 0)
  {
      std::cerr << "Couldn't read conf file "<< ret << std::endl;
  }
  ret = cluster.connect();
  if(ret < 0)
  {
      std::cerr << "Couldn't connect to cluster "<< ret << std::endl;
  }
  else
  {
      std::cout << "Connected to Cluster"<< std::endl;
  }

  ret = cluster.ioctx_create(pool_name.c_str(), io_ctx);

  if(ret < 0)
  {
      std::cerr << "Couldn't Create IO_CTX"<< ret << std::endl;
  }
  ret = libradosstriper::RadosStriper::striper_create(io_ctx,rs);
  if(ret < 0)
  {
      std::cerr << "Couldn't Create RadosStriper"<< ret << std::endl;
      delete rs;
  }
  uint64_t alignment = 0;
  ret = io_ctx.pool_required_alignment2(&alignment);
  if(ret < 0)
  {
      std::cerr << "IO_CTX didn't give alignment "<< ret
          << "\n Is this an erasure coded pool? "<< std::endl;

      delete rs;
      io_ctx.close();
      cluster.shutdown();
      return EXIT_FAILURE;
  }
  std::cout << "Pool alignment: "<< alignment << std::endl;
  rs->set_object_layout_stripe_unit(alignment);
  // how many objects are we striping across?
  rs->set_object_layout_stripe_count(strip_count);
  // how big should each object be?
  rs->set_object_layout_object_size(obj_size);

  std::string err = "no_err";
  librados::bufferlist bl;
  bl.read_file(fname.c_str(),&err);
  if(err != "no_err")
  {
      std::cout << "Error reading file into bufferlist: "<< err << std::endl;
      delete rs;
      io_ctx.close();
      cluster.shutdown();
      return EXIT_FAILURE;
  }

  std::cout << "Writing: " << fname << "\nas: "<< obj_name << std::endl;
  rs->write_full(obj_name,bl);
  std::cout << "done with: " << fname << std::endl;

  delete rs;
  io_ctx.close();
  cluster.shutdown();
}
