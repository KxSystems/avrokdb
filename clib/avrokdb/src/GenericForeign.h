#pragma once

#include <memory>
#include <set>
#include <mutex>

#include "HelperFunctions.h"


template <typename T>
class ForeignSet
{
public:
  class Foreign
  {
  private:
    std::shared_ptr<T> foreign;

  private:
    // Constructor is private and only used by KdbConstructor static function 
    Foreign(std::shared_ptr<T> foreign_) :
      foreign(foreign_)
    {};

  public:
    // KdbDestructor is registered as foreign object destructor callback
    static K KdbDestructor(K foreign_);

    // Creates the Foreign object and wraps it in a kdb
    // foreign object
    static K KdbConstructor(std::shared_ptr<T> foreign_);

    // Getter functions for object members
    std::shared_ptr<T> GetForeign();
  };

private:
  std::set<Foreign*> valid_foreign_data;
  std::mutex valid_foreign_data_mutex;

public:
  // InvalidForeign is thrown if an invalid foreign object is specified
  class InvalidForeign : public std::invalid_argument
  {
  public:
    InvalidForeign(std::string message) : std::invalid_argument(message.c_str())
    {};
  };

  void Add(Foreign* foreign_data)
  {
    std::lock_guard<std::mutex> lg(valid_foreign_data_mutex);
    valid_foreign_data.insert(foreign_data);
  }

  void Remove(Foreign* foreign_data)
  {
    std::lock_guard<std::mutex> lg(valid_foreign_data_mutex);
    valid_foreign_data.erase(foreign_data);
  }

  bool Find(Foreign* foreign_data)
  {
    std::lock_guard<std::mutex> lg(valid_foreign_data_mutex);
    return valid_foreign_data.find(foreign_data) != valid_foreign_data.end();
  }

  static ForeignSet& Instance()
  {
    static ForeignSet foreign_set;
    return foreign_set;
  }

  void ClearAll()
  {
    std::lock_guard<std::mutex> lg(valid_foreign_data_mutex);
    for (auto i : valid_foreign_data)
      delete i;

    valid_foreign_data.clear();
  }

  ~ForeignSet()
  {
    ForeignSet::Instance().ClearAll();
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
};

template<typename T>
K ForeignSet<T>::Foreign::KdbDestructor(K foreign_)
{
  KDB_EXCEPTION_TRY;

  auto* foreign_data = ForeignSet::Instance().Get(foreign_);

  // Delete the Foreign object.  This will decrement the refcounts of the
  // shared pointers to allow them to be deleted. 
  ForeignSet::Instance().Remove(foreign_data);
  delete foreign_data;

  return (K)0;

  KDB_EXCEPTION_CATCH;
}

template<typename T>
K ForeignSet<T>::Foreign::KdbConstructor(std::shared_ptr<T> foreign_)
{
  // Use a raw new rather than a smart point since kdb will be controlling the
  // lifetime via its refcount
  auto* foreign_data = new Foreign(foreign_);
  ForeignSet::Instance().Add(foreign_data);
  K result = knk(2, Foreign::KdbDestructor, foreign_data);
  result->t = 112;

  return result;
}

template<typename T>
std::shared_ptr<T> ForeignSet<T>::Foreign::GetForeign()
{
  return foreign;
}

template <typename T>
std::shared_ptr<T> GetForeign(K foreign)
{
  return ForeignSet<T>::Instance().Get(foreign)->GetForeign();
}

