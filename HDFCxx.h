#if !defined(HDFCxx_h)
#define HDFCxx_h

#include "hdf5.h"
#include "mpi/mpicpp.h"
#include <stdexcept>
#include <vector>

namespace cce::tf::hdf5 {


inline hid_t string_type() {
  auto atype = H5Tcopy(H5T_C_S1);
  H5Tset_size(atype, H5T_VARIABLE);
  H5Tset_cset(atype, H5T_CSET_UTF8);
  H5Tset_strpad(atype,H5T_STR_NULLTERM);
  return atype;
}

template<typename T> constexpr hid_t H5filetype_for = -1;
template<> inline hid_t H5filetype_for<int> = H5T_STD_I32LE;
template<> inline hid_t H5filetype_for<unsigned int> = H5T_STD_U32LE;
template<> inline hid_t H5filetype_for<char> = H5T_STD_I8LE;
template<> inline hid_t H5filetype_for<size_t> = H5T_STD_U64LE;
template<> inline hid_t H5filetype_for<unsigned long long> = H5T_STD_U64LE;

template<typename T> constexpr hid_t H5memtype_for = -1;
template<> inline hid_t H5memtype_for<int> = H5T_NATIVE_INT;
template<> inline hid_t H5memtype_for<unsigned int> = H5T_NATIVE_UINT;
template<> inline hid_t H5memtype_for<char> = H5T_NATIVE_CHAR;
template<> inline hid_t H5memtype_for<size_t> = H5T_NATIVE_ULLONG;
template<> inline hid_t H5memtype_for<unsigned long long> = H5T_NATIVE_ULLONG;
template<> inline hid_t H5memtype_for<std::string> = string_type();

//wrapper for File
class File {
  public:
    static File create(const char *name) {
      return File(H5Fcreate(name, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT));
    }

    static File parallel_create(const char *name) {
        MPI_Info info_;
        MPI_Info_create(&info_);
        hid_t plist_id_ = H5Pcreate (H5P_FILE_ACCESS);
        auto  ret = H5Pset_fapl_mpio(plist_id_, MPI_COMM_SELF, info_);
        std::cout << "Trying to create file in MPI mode\n"; 
        return File(H5Fcreate(name, H5F_ACC_TRUNC, H5P_DEFAULT, plist_id_));
    }

    static File open(const char *name) {
      return File(H5Fopen(name, H5F_ACC_RDONLY, H5P_DEFAULT));
    }
    ~File() {
     // H5Pclose(plist_id_);
      H5Fclose(file_);
    }
    operator hid_t() const {return file_;}
  private:
    explicit File(hid_t fid):file_(fid) { 
      if (file_ < 0) {
        throw std::runtime_error("Unable to create/open the file\n");
      }
    }
   // hid_t plist_id_;
    hid_t file_;
   // MPI_Info info_;
};
//wrapper for Group
class Group {
  public:
    static Group create(hid_t id, const char *name) {
      return Group(H5Gcreate2(id, name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT));
    } 
    static Group open(hid_t id, const char *name) {
      return Group(H5Gopen2(id, name, H5P_DEFAULT));
    }
    ~Group() {
      H5Gclose(group_);
    }
    operator hid_t() const {return group_;}
  private:
    explicit Group(hid_t gid):group_(gid) {
      if (group_ < 0) {
        throw std::runtime_error("Unable to create/open the group\n");
      }
    }
    hid_t group_;
};

//wrapper for Dataset
class Dataset {
  public: 
    template<typename T> 
    static Dataset create(hid_t id, const char *name, hid_t space_id, hid_t dcpl_id) {
    
     return Dataset(H5Dcreate2(id, name, H5filetype_for<T>, space_id, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)); 
    } 
    static Dataset open(hid_t id, const char *name){
      return Dataset(H5Dopen2(id, name, H5P_DEFAULT));}
    ~Dataset() { 
       H5Dclose(dataset_);
    }
    operator hid_t() const {return dataset_;}
    
    template<typename T>
    auto 
    write(hid_t dspace, hid_t filespace, std::vector<T> const & data) {
      return H5Dwrite(dataset_, H5memtype_for<T>, dspace, filespace, H5P_DEFAULT, &data[0]);
    }
  
    auto set_extent(hsize_t const *dims) {
     auto err = H5Dset_extent(dataset_, dims);
     if (err < 0) {
       throw std::runtime_error("Unable to extend the dataset\n");
     }
    }
 
  private: 
      explicit Dataset(hid_t ds_id):dataset_(ds_id) {
      if (dataset_ < 0) {
        throw std::runtime_error("Unable to create/open the dataset\n");
      }
      }
      hid_t dataset_;
};

//wrapper for Attributes
class Attribute {
  public: 
    template<typename T>
    static Attribute create(hid_t id, const char *name, hid_t aid) {
     return Attribute(H5Acreate2(id, name, H5memtype_for<T>, aid, H5P_DEFAULT, H5P_DEFAULT)); 
    } 
    static Attribute open(hid_t id, const char *name){
      return Attribute(H5Aopen(id, name, H5P_DEFAULT));
    }
    ~Attribute() { 
       H5Aclose(attribute_);
    }
    operator hid_t() const {return attribute_;}
    template<typename T>
    auto write(T const & data){
        return H5Awrite(attribute_, H5memtype_for<T>, &data);
    } 
   private: 
     explicit Attribute(hid_t a_id):attribute_(a_id) {
       if (attribute_ < 0) {
         throw std::runtime_error("Unable to create/open the attribute\n");
       }
      }
      hid_t attribute_;
};

//wrapper for Dataspace
//

class Dataspace {
  public:
    static Dataspace create_simple(hsize_t ndims, hsize_t const *dims, hsize_t const *maxdims) {
      return Dataspace(H5Screate_simple(ndims, dims, maxdims));
    }
    static Dataspace get_space(hid_t hid) {
      return Dataspace(H5Dget_space(hid));
    }
    static Dataspace create_scalar() {
      return Dataspace(H5Screate(H5S_SCALAR));
    }

    auto select_hyperslab(hsize_t const *dims, hsize_t const *slab) {
      auto err = H5Sselect_hyperslab(dspace_, H5S_SELECT_SET, dims, NULL, slab, NULL);
      if (err < 0) {
        throw std::runtime_error("Unable to select hyperslab\n");
      }
    } 
    ~Dataspace() {
      H5Sclose(dspace_);
    }
    operator hid_t() const {return dspace_;}
  private:
    explicit Dataspace(hid_t s):dspace_(s) {
      if (dspace_ < 0) {
        throw std::runtime_error("Unable to create/open the dataspace\n");
      }
    }
    hid_t dspace_;
};


//wrapper for Propertylist
//

class Property {
  public:
    static Property create() {
      return Property(H5Pcreate(H5P_DATASET_CREATE));
    }
    void set_chunk(hsize_t ndims, hsize_t const *dims) {
      auto err = H5Pset_chunk(prop_, ndims, dims);
      if (err < 0) {
        throw std::runtime_error("Unable to set chunk size\n");
      }
    } 
    ~Property() {
      H5Pclose(prop_);
    }
    operator hid_t() const {return prop_;}
  private:
    explicit Property(hid_t p):prop_(p) {
      if (prop_ < 0) {
        throw std::runtime_error("Unable to create property\n");
      }
    }
    hid_t prop_;
};

class DataXfer{
    public:
      static DataXfer create(){
	 return DataXfer(H5Pcreate(H5P_DATASET_XFER));

  }
  ~DataXfer(){H5Pclose(xf_id_);}
  operator hid_t() const {return xf_id_;};
  void set_mpio(){
     H5Pset_dxpl_mpio(xf_id_,H5FD_MPIO_COLLECTIVE);
  }
  private:
   explicit DataXfer(hid_t xf_id):xf_id_(xf_id){
      if(xf_id_<0)
	     throw std::runtime_error("Unable to create xfer property"); 
   }
   hid_t xf_id_;
};
}
#endif
