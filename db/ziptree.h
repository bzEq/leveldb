// Copyright (c) 2018 Kai Luo <gluokai@gmail.com>. All rights reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_ZIPTREE_H_
#define STORAGE_LEVELDB_DB_ZIPTREE_H_

#include <assert.h>
#include <stdlib.h>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <thread>

#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

class Arena;

template <typename Key, typename Comparator>
class ZipTree {
 private:
  struct Node {
    explicit Node(const Key& k, int r)
        : key(k), rank(r), left(nullptr), right(nullptr), parent(nullptr) {}

    Key const key;
    int rank;
    Node *left, *right, *parent;
  };

 private:
  enum { kMaxRank = 11 };

  Node* Find(const Key& key) const {
    Node* current = root_;
    while (current) {
      if (compare_(key, current->key) == 0) {
        break;
      }
      if (compare_(key, current->key) < 0) {
        current = current->left;
      } else {
        current = current->right;
      }
    }
    return current;
  }

  size_t size(Node* root) const {
    if (root == nullptr) {
      return 0;
    }
    return size(root->left) + size(root->right) + 1;
  }

  size_t height(Node* root) const {
    if (root == nullptr) {
      return 0;
    }
    return std::max(height(root->left), height(root->right)) + 1;
  }

  bool CheckConsistency(Node* root) const {
    if (root == nullptr) {
      return true;
    }
    bool result = true;
    if (root->parent) {
      result =
          result && (root == root->parent->left || root == root->parent->right);
    }
    if (root->left) {
      result = result && (compare_(root->left->key, root->key) < 0) &&
               CheckConsistency(root->left);
    }
    if (root->right) {
      result = result && (compare_(root->right->key, root->key) > 0) &&
               CheckConsistency(root->right);
    }
    return result;
  }

  Node* NewNode(const Key& key, int rank) {
    char* mem = arena_->AllocateAligned(sizeof(Node));
    return new (mem) Node(key, rank);
  }

  int RandomRank() {
    static const unsigned int kBranching = 6;
    int rank = 0;
    while (rnd_.Next() % kBranching == 0 && rank < kMaxRank) {
      ++rank;
    }
    return rank;
  }

  Node* RecursiveInsert(Node* x, Node* root) {
    if (root == nullptr) {
      return x;
    }
    if (compare_(x->key, root->key) < 0) {
      if (RecursiveInsert(x, root->left) == x) {
        if (x->rank < root->rank) {
          root->left = x;
          x->parent = root;
        } else {
          x->parent = root->parent;
          root->left = x->right;
          if (x->right) {
            x->right->parent = root;
          }
          x->right = root;
          root->parent = x;
          return x;
        }
      }
    } else {
      if (RecursiveInsert(x, root->right) == x) {
        if (x->rank <= root->rank) {
          root->right = x;
          x->parent = root;
        } else {
          x->parent = root->parent;
          root->right = x->left;
          if (x->left) {
            x->left->parent = root;
          }
          x->left = root;
          root->parent = x;
          return x;
        }
      }
    }
    return root;
  }

  ZipTree(const ZipTree&);

  void operator=(const ZipTree&);

 public:
  explicit ZipTree(Comparator cmp, Arena* arena)
      : compare_(cmp), arena_(arena), root_(nullptr), rnd_(0xc0debabe) {}

  void Insert(const Key& key);

  bool Contains(const Key& key) const;

  size_t size() const;

  size_t height() const;

  bool CheckConsistency() const;

  friend class Iterator;

  class Iterator {
   private:
    const ZipTree* tree_;
    Node* node_;

   public:
    explicit Iterator(const ZipTree* tree) : tree_(tree), node_(nullptr) {}

    bool Valid() const {
      std::shared_lock<std::shared_mutex> _(tree_->mu_);
      return node_ != nullptr;
    }

    const Key& key() const {
      std::shared_lock<std::shared_mutex> _(tree_->mu_);
      return node_->key;
    }

    void Next() {
      std::shared_lock<std::shared_mutex> _(tree_->mu_);
      if (node_->right) {
        Node* cursor = node_->right;
        while (cursor) {
          node_ = cursor;
          cursor = cursor->left;
        }
      } else if (node_->parent) {
        Node* parent = node_->parent;
        if (node_ == parent->right) {
          while (parent && node_ == parent->right) {
            node_ = parent;
            parent = node_->parent;
          }
          if (parent && node_ == parent->left) {
            node_ = parent;
          } else {
            node_ = nullptr;
          }
        } else {
          assert(parent->left == node_);
          node_ = parent;
        }
      } else {
        assert(node_ == tree_->root_);
        node_ = nullptr;
      }
    }

    void Prev() {
      std::shared_lock<std::shared_mutex> _(tree_->mu_);
      if (node_->left) {
        Node* cursor = node_->left;
        while (cursor) {
          node_ = cursor;
          cursor = cursor->right;
        }
      } else if (node_->parent) {
        Node* parent = node_->parent;
        if (node_ == parent->left) {
          while (parent && node_ == parent->left) {
            node_ = parent;
            parent = node_->parent;
          }
          if (parent && node_ == parent->right) {
            node_ = parent;
          } else {
            node_ = nullptr;
          }
        } else {
          node_ = parent;
        }
      } else {
        node_ = nullptr;
      }
    }

    void Seek(const Key& target) {
      std::shared_lock<std::shared_mutex> _(tree_->mu_);
      Node* cursor = tree_->root_;
      while (cursor) {
        node_ = cursor;
        if (tree_->compare_(target, node_->key) == 0) {
          return;
        }
        if (tree_->compare_(target, node_->key) < 0) {
          cursor = cursor->left;
        } else {
          cursor = cursor->right;
        }
      }
      if (not node_) {
        return;
      }
      if (tree_->compare_(target, node_->key) > 0) {
        Next();
      }
    }

    void SeekToFirst() {
      std::shared_lock<std::shared_mutex> _(tree_->mu_);
      Node* cursor = tree_->root_;
      while (cursor) {
        node_ = cursor;
        cursor = cursor->left;
      }
    }

    void SeekToLast() {
      std::shared_lock<std::shared_mutex> _(tree_->mu_);
      Node* cursor = tree_->root_;
      while (cursor) {
        node_ = cursor;
        cursor = cursor->right;
      }
    }
  };

 private:
  Comparator const compare_;
  Arena* const arena_;
  Node* root_;
  Random rnd_;
  mutable std::shared_mutex mu_;
};  // namespace leveldb

template <typename Key, typename Comparator>
inline void ZipTree<Key, Comparator>::Insert(const Key& key) {
  std::unique_lock<std::shared_mutex> _(mu_);
  Node* x = NewNode(key, RandomRank());
  root_ = RecursiveInsert(x, root_);
}

template <typename Key, typename Comparator>
inline bool ZipTree<Key, Comparator>::Contains(const Key& key) const {
  std::shared_lock<std::shared_mutex> _(mu_);
  return Find(key) != nullptr;
}

template <typename Key, typename Comparator>
inline size_t ZipTree<Key, Comparator>::size() const {
  std::shared_lock<std::shared_mutex> _(mu_);
  return size(root_);
}

template <typename Key, typename Comparator>
inline size_t ZipTree<Key, Comparator>::height() const {
  std::shared_lock<std::shared_mutex> _(mu_);
  return height(root_);
}

template <typename Key, typename Comparator>
inline bool ZipTree<Key, Comparator>::CheckConsistency() const {
  std::shared_lock<std::shared_mutex> _(mu_);
  return CheckConsistency(root_);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_ZIPTREE_H_
