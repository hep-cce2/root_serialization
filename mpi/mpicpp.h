#ifndef MPICPP_H
#define MPICPP_H

#include "mpi.h"
#include <iostream>

template <typename T> struct mpi_type {};

template <> struct mpi_type<int> {static const MPI_Datatype value = MPI_INT;};
template <> struct mpi_type<char> {static const MPI_Datatype value = MPI_CHAR;};

class Mpi {
public:
  Mpi(int argc, char* argv[]);
  ~Mpi();

  int
  rank() const
  {
    return rank_;
  }
  int
  np() const
  {
    return np_;
  }
  int
  hostid() const
  {
    return hostid_;
  }
  std::string
  name() const
  {
    return name_;
  }

  int
  rank_group() const
  {
    return rank_group_;
  }
  int
  np_group() const
  {
    return np_group_;
  }
  MPI_Comm
  comm_group() const
  {
    return comm_;
  }
  MPI_Comm
  comm() const
  {
    return MPI_COMM_WORLD;
  }

  void barrier();
  void barrier_group();
  template <typename T>
  int broadcast(T* v, int root) {
    return MPI_Bcast(v, 1, mpi_type<T>::value, root, MPI_COMM_WORLD);    
  }

  template <std::size_t N, typename T>
  int broadcast(std::array<T, N>* buffer, int root) {
     return MPI_Bcast(buffer, N, mpi_type<T>::value, root, MPI_COMM_WORLD);
  }

private:
  int np_;
  int rank_;
  int rank_group_;
  int np_group_;
  int hostid_;
  std::string name_;
  MPI_Comm comm_;
};

inline 
Mpi::Mpi(int argc, char* argv[])
{
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  //assert(provided == MPI_THREAD_MULTIPLE);
  MPI_Comm_size(MPI_COMM_WORLD, &np_);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank_);
  hostid_ = (unsigned int)(gethostid());

  unsigned int group_size = 1;

  hostid_ = (rank_) / group_size;

  int name_sz = 0;
  char name_buf[MPI_MAX_PROCESSOR_NAME];
  MPI_Get_processor_name(name_buf, &name_sz);
  name_ = name_buf;

  MPI_Comm_split(MPI_COMM_WORLD, hostid_, rank_, &comm_);
  MPI_Comm_rank(comm_, &rank_group_);
  MPI_Comm_size(comm_, &np_group_);
}

inline
Mpi::~Mpi()
{
  //MPI_Finalize();
}

inline
void
Mpi::barrier()
{
  MPI_Barrier(MPI_COMM_WORLD);
}

inline
void
Mpi::barrier_group()
{
  MPI_Barrier(comm_);
}

#endif
