// Copyright (c) 2016 Kai Luo <gluokai@gmail.com>. All rights reserved.

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SPLAYTREE_H_
#define STORAGE_LEVELDB_DB_SPLAYTREE_H_
#include <assert.h>
#include <mutex>
#include <stdlib.h>

#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

class Arena;

template <typename Key, typename Comparator>
class SplayTree {
 private:
  struct Node;

 public:
  explicit SplayTree(Comparator cmp, Arena *arena)
      : comparator_(cmp), root_(nullptr), size_(0), arena_(arena) {}
  void Insert(const Key &key);
  bool Delete(const Key &key);
  bool Contains(const Key &key) const;
  size_t Size() const { return size_; }
  ~SplayTree() { delete root_; }

  class Iterator {
   public:
    explicit Iterator(SplayTree *);
    const Key &operator*() const;
    const Key &key() const;
    bool Valid() const;
    void Next();
    void Prev();
    void Seek(const Key &target);
    void SeekToFirst();
    void SeekToLast();

   private:
    SplayTree *tree_;
    Node *node_;
  };

 private:
  enum NodeSide {
    kLeft,
    kRight,
  };

  struct Node {
    Node *parent;
    Node *child[2];
    Key key;
    Node(const Key &k) : parent(nullptr), child{nullptr, nullptr}, key(k) {}
    ~Node() {
      delete child[0];
      delete child[1];
    }
  };

  void Collect(Node *n);

  Node *SubMinimum(Node *n);

  Node *SubMaximum(Node *n);

  Node *Find(const Key &key) const;
  Node *FindGreaterOrEqual(const Key &key);

  Node *Prev(Node *n);
  Node *Next(Node *n);
  Node *First();
  Node *Last();

  void Splay(Node *n);

  void Rotate(Node *n, NodeSide s);

  Comparator comparator_;
  Node *root_;
  size_t size_;
  Arena *arena_;
  mutable std::mutex mu_;
};

template <typename Key, typename Comparator>
inline void SplayTree<Key, Comparator>::Insert(const Key &key) {
  std::unique_lock<std::mutex> _(mu_);
  if (!root_) {
    root_ = new Node(key);
    ++size_;
    return;
  }
  Node *current = root_;
  Node *parent = current->parent;
  NodeSide side;
  assert(!parent);
  while (current) {
    parent = current;
    if (comparator_(key, current->key) == 0) {
      return;
    }
    if (comparator_(current->key, key) < 0) {
      side = kRight;
      current = current->child[kRight];
    } else {
      side = kLeft;
      current = current->child[kLeft];
    }
  }
  current = new Node(key);
  current->parent = parent;
  if (!parent) {
    root_ = current;
  } else {
    parent->child[side] = current;
  }

  Splay(current);
  assert(root_ == current);
  assert(root_->parent == nullptr);

  ++size_;
}

template <typename Key, typename Comparator>
inline bool SplayTree<Key, Comparator>::Contains(const Key &key) const {
  std::unique_lock<std::mutex> _(mu_);
  return Find(key) != nullptr;
}

template <typename Key, typename Comparator>
inline bool SplayTree<Key, Comparator>::Delete(const Key &key) {
  Node *n = Find(key);
  if (!n) {
    return false;
  }
  Splay(n);
  assert(root_ == n);
  assert(n->parent == nullptr);
  if (!n->child[kLeft]) {
    root_ = n->child[kRight];
    if (root_) {
      root_->parent = nullptr;
    }
    Collect(n);
  } else if (!n->child[kRight]) {
    root_ = n->child[kLeft];
    if (root_) {
      root_->parent = nullptr;
    }
    Collect(n);
  } else {
    Node *c = SubMinimum(n->child[kRight]);
    if (c->parent == n) {
      assert(n->child[kRight] == c);
      assert(c->child[kLeft] == nullptr);
      c->child[kLeft] = n->child[kLeft];
      assert(c->child[kLeft]);
      c->child[kLeft]->parent = c;
      c->parent = n->parent;
      Collect(n);
    } else {
      assert(c->parent->child[kLeft] == c);
      assert(c->child[kLeft] == nullptr);
      c->parent->child[kLeft] = c->child[kRight];
      if (c->child[kRight]) {
        c->child[kRight]->parent = c->parent;
      }
      assert(n->parent == nullptr);

      c->parent = n->parent;

      c->child[kRight] = n->child[kRight];
      c->child[kRight]->parent = c;
      assert(c->child[kRight]->parent == c);

      c->child[kLeft] = n->child[kLeft];
      c->child[kLeft]->parent = c;
      assert(c->child[kLeft]->parent == c);

      Collect(n);
    }
    root_ = c;
  }
  if (root_) {
    assert(root_->parent == nullptr);
  }
  --size_;
  return true;
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::Next(Node *node) {
  // std::unique_lock<std::mutex> _(mu_);
  if (node->child[kRight]) {
    return SubMinimum(node->child[kRight]);
  }
  if (node->parent && node == node->parent->child[kLeft]) {
    return node->parent;
  }
  while (node->parent) {
    if (node == node->parent->child[kLeft]) {
      break;
    }
    node = node->parent;
  }
  return node->parent;
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::Prev(Node *node) {
  // std::unique_lock<std::mutex> _(mu_);
  if (node->child[kLeft]) {
    return SubMaximum(node->child[kLeft]);
  }
  if (node->parent && node == node->parent->child[kRight]) {
    return node->parent;
  }
  while (node->parent) {
    if (node == node->parent->child[kRight]) {
      break;
    }
    node = node->parent;
  }
  return node->parent;
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::FindGreaterOrEqual(const Key &key) {
  // std::unique_lock<std::mutex> _(mu_);
  Node *current = root_;
  Node *prev = nullptr;
  while (current) {
    prev = current;
    if (comparator_(current->key, key) == 0) {
      break;
    }
    if (comparator_(current->key, key) < 0) {
      current = current->child[kRight];
    } else {
      current = current->child[kLeft];
    }
  }
  if (current) {
    return current;
  }
  if (!prev) {
    return prev;
  }
  if (comparator_(key, prev->key) < 0) {
    return prev;
  }
  return Next(prev);
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::Find(const Key &key) const {
  // std::unique_lock<std::mutex> _(mu_);
  Node *current = root_;
  while (current) {
    if (current->key == key) {
      break;
    }
    if (comparator_(current->key, key) < 0) {
      current = current->child[kRight];
    } else {
      current = current->child[kLeft];
    }
  }
  return current;
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::SubMinimum(SplayTree<Key, Comparator>::Node *n) {
  while (n->child[kLeft]) {
    n = n->child[kLeft];
  }
  return n;
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::SubMaximum(SplayTree<Key, Comparator>::Node *n) {
  while (n->child[kRight]) {
    n = n->child[kRight];
  }
  return n;
}

template <typename Key, typename Comparator>
inline void
SplayTree<Key, Comparator>::Splay(SplayTree<Key, Comparator>::Node *n) {
  while (n->parent) {
    if (!n->parent->parent) {
      if (n == n->parent->child[kLeft]) {
        Rotate(n->parent, kLeft);
      } else {
        assert(n == n->parent->child[kRight]);
        Rotate(n->parent, kRight);
      }
    } else if (n == n->parent->child[kLeft] &&
               n->parent == n->parent->parent->child[kLeft]) {
      Rotate(n->parent->parent, kLeft);
      Rotate(n->parent, kLeft);
    } else if (n == n->parent->child[kRight] &&
               n->parent == n->parent->parent->child[kRight]) {
      Rotate(n->parent->parent, kRight);
      Rotate(n->parent, kRight);
    } else if (n == n->parent->child[kLeft] &&
               n->parent == n->parent->parent->child[kRight]) {
      Rotate(n->parent, kLeft);
      Rotate(n->parent, kRight);
    } else {
      assert(n == n->parent->child[kRight] &&
             n->parent == n->parent->parent->child[kLeft]);
      Rotate(n->parent, kRight);
      Rotate(n->parent, kLeft);
    }
  }
}

template <typename Key, typename Comparator>
inline void
SplayTree<Key, Comparator>::Rotate(SplayTree<Key, Comparator>::Node *n,
                                   SplayTree<Key, Comparator>::NodeSide s) {
  NodeSide os = static_cast<NodeSide>(1 - s);
  Node *c = n->child[s];
  if (!c) {
    return;
  }
  n->child[s] = c->child[os];
  if (c->child[os]) {
    c->child[os]->parent = n;
  }
  c->parent = n->parent;
  if (!n->parent) {
    assert(root_ == n);
    root_ = c;
  } else if (n->parent->child[kLeft] == n) {
    n->parent->child[kLeft] = c;
  } else {
    assert(n->parent->child[kRight] == n);
    n->parent->child[kRight] = c;
  }
  c->child[os] = n;
  n->parent = c;
}

template <typename Key, typename Comparator>
inline void
SplayTree<Key, Comparator>::Collect(SplayTree<Key, Comparator>::Node *n) {
  if (n) {
    n->child[0] = nullptr;
    n->child[1] = nullptr;
    delete n;
  }
}

template <typename Key, typename Comparator>
inline SplayTree<Key, Comparator>::Iterator::Iterator(
    SplayTree<Key, Comparator> *tree) {
  tree_ = tree;
  node_ = nullptr;
}

template <typename Key, typename Comparator>
inline const Key &SplayTree<Key, Comparator>::Iterator::operator*() const {
  std::unique_lock<std::mutex> _(tree_->mu_);
  assert(node_);
  return node_->key;
}

template <typename Key, typename Comparator>
inline const Key &SplayTree<Key, Comparator>::Iterator::key() const {
  std::unique_lock<std::mutex> _(tree_->mu_);
  assert(node_);
  return node_->key;
}

template <typename Key, typename Comparator>
inline bool SplayTree<Key, Comparator>::Iterator::Valid() const {
  std::unique_lock<std::mutex> _(tree_->mu_);
  return node_ != nullptr;
}

template <typename Key, typename Comparator>
inline void SplayTree<Key, Comparator>::Iterator::Prev() {
  std::unique_lock<std::mutex> _(tree_->mu_);
  node_ = tree_->Prev(node_);
}

template <typename Key, typename Comparator>
inline void SplayTree<Key, Comparator>::Iterator::Next() {
  std::unique_lock<std::mutex> _(tree_->mu_);
  node_ = tree_->Next(node_);
}

template <typename Key, typename Comparator>
inline void SplayTree<Key, Comparator>::Iterator::SeekToFirst() {
  std::unique_lock<std::mutex> _(tree_->mu_);
  if (tree_->root_ && tree_->root_->child[kLeft]) {
    node_ = tree_->SubMinimum(tree_->root_->child[kLeft]);
  } else {
    node_ = tree_->root_;
  }
}

template <typename Key, typename Comparator>
inline void SplayTree<Key, Comparator>::Iterator::Seek(const Key &target) {
  std::unique_lock<std::mutex> _(tree_->mu_);
  node_ = tree_->FindGreaterOrEqual(target);
}

template <typename Key, typename Comparator>
inline void SplayTree<Key, Comparator>::Iterator::SeekToLast() {
  std::unique_lock<std::mutex> _(tree_->mu_);
  if (tree_->root_ && tree_->root_->child[kRight]) {
    node_ = tree_->SubMaximum(tree_->root_->child[kRight]);
  } else {
    node_ = tree_->root_;
  }
}

}  // namespace leveldb
#endif
