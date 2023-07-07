#pragma once

#include <memory>
#include <set>
#include <shared_mutex>

#include "HelperFunctions.h"


template <typename T>
class ForeignSet
{
private:
  class Foreign
  {
  private:
    std::shared_ptr<T> foreign;

  public:
    Foreign(std::shared_ptr<T> foreign_) :
      foreign(foreign_)
    {};

    // Getter functions for object members
    std::shared_ptr<T> GetForeign()
    {
      return foreign;
    }
  };

private:
  std::set<Foreign*> valid_foreign_data;
  std::shared_timed_mutex valid_foreign_data_mutex;

public:
  // InvalidForeign is thrown if an invalid foreign object is specified
  class InvalidForeign : public std::invalid_argument
  {
  public:
    InvalidForeign(std::string message) : std::invalid_argument(message.c_str())
    {};
  };

private:
  void Add(Foreign* foreign_data)
  {
    std::unique_lock<std::shared_timed_mutex> lock(valid_foreign_data_mutex);
    valid_foreign_data.insert(foreign_data);
  }

  void Remove(Foreign* foreign_data)
  {
    std::unique_lock<std::shared_timed_mutex> lock(valid_foreign_data_mutex);
    valid_foreign_data.erase(foreign_data);
  }

  bool Find(Foreign* foreign_data)
  {
    std::shared_lock<std::shared_timed_mutex> lock(valid_foreign_data_mutex);
    return valid_foreign_data.find(foreign_data) != valid_foreign_data.end();
  }

  ~ForeignSet()
  {
    std::unique_lock<std::shared_timed_mutex> lock(valid_foreign_data_mutex);
    for (auto i : valid_foreign_data) {
      std::cout << "Destructing " << std::hex << i << std::endl;
      delete i;
    }

    valid_foreign_data.clear();
  }

public:
  static ForeignSet& Instance()
  {
    static ForeignSet foreign_set;
    return foreign_set;
  }

  // Returns the Foreign object that has been wrapped in a kdb foreign
  // object
  Foreign* Get(K foreign)
  {
    if (foreign->t != 112)
      throw InvalidForeign("foreign expected 112h");

    if (foreign->n != 2)
      throw InvalidForeign("foreign length expected 2");

    auto* foreign_data = (Foreign*)kK(foreign)[1];
    if (!Find(foreign_data))
      throw InvalidForeign(std::string("foreign does not contain ") + typeid(T).name());

    return foreign_data;
  }

  // Creates the Foreign object and wraps it in a kdb
  // foreign object
  K KdbConstructor(std::shared_ptr<T> foreign_);

  // KdbDestructor is registered as foreign object destructor callback
  static K KdbDestructor(K foreign_);
};

template<typename T>
K ForeignSet<T>::KdbDestructor(K foreign_)
{
  KDB_EXCEPTION_TRY;

  auto* foreign_data = ForeignSet::Instance().Get(foreign_);

  // Delete the Foreign object.  This will decrement the refcounts of the
  // shared pointers to allow them to be deleted. 
  std::cout << "Destructing " << std::hex << foreign_data << std::endl;
  ForeignSet::Instance().Remove(foreign_data);
  delete foreign_data;

  return (K)0;

  KDB_EXCEPTION_CATCH;
}

template<typename T>
K ForeignSet<T>::KdbConstructor(std::shared_ptr<T> foreign_)
{
  // Use a raw new rather than a smart point since kdb will be controlling the
  // lifetime via its refcount
  auto* foreign_data = new Foreign(foreign_);
  std::cout << "Constructing " << std::hex << foreign_data << std::endl;
  Add(foreign_data);
  K result = knk(2, KdbDestructor, foreign_data);
  result->t = 112;

  return result;
}

template <typename T>
std::shared_ptr<T> GetForeign(K foreign)
{
  return ForeignSet<T>::Instance().Get(foreign)->GetForeign();
}

template <typename T>
K MakeForeign(std::shared_ptr<T> foreign)
{
  return ForeignSet<T>::Instance().KdbConstructor(foreign);
}

template <typename T>
K MakeForeign(T foreign)
{
  return ForeignSet<T>::Instance().KdbConstructor(std::make_shared<T>(foreign));
}
