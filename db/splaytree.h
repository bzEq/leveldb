// Copyright (c) 2016 Kai Luo <gluokai@gmail.com>. All rights reserved.

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SPLAYTREE_H_
#define STORAGE_LEVELDB_DB_SPLAYTREE_H_
#include <assert.h>
#include <atomic>
#include <iostream>
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
    bool inserted;
    Node *parent;
    std::array<Node *, 2> child;
    // helper pointer to assist iteration
    Node *ancestor_prev;
    Node *ancestor_next;
    const Key key;
    Node(const Key &k)
        : inserted(false), parent(nullptr), child{{nullptr, nullptr}}, key(k),
          ancestor_prev(nullptr), ancestor_next(nullptr) {}

    ~Node() {
      delete child[0];
      delete child[1];
    }
  };

  void Collect(Node *n);

  Node *UnlockSubMinimum(Node *n);

  Node *UnlockSubMaximum(Node *n);

  Node *UnlockFind(const Key &key) const;
  Node *FindGreaterOrEqual(const Key &key);
  Node *UnlockFindGreaterOrEqual(const Key &key);

  Node *Prev(Node *n);
  Node *Next(Node *n);
  Node *UnlockNext(Node *n);
  Node *UnlockPrev(Node *n);
  Node *First();
  Node *Last();

  void Splay(Node *n);

  void Rotate(Node *n, NodeSide s);

  Comparator comparator_;
  Node *root_;
  size_t size_;
  Arena *arena_;
  mutable port::RWLock rwlock_;
};

template <typename Key, typename Comparator>
inline void SplayTree<Key, Comparator>::Insert(const Key &key) {
  Node *insert = new Node(key);

  port::RWLockRDGuard insert_guard(&rwlock_);
  auto current = &root_;
  NodeSide side;
  assert(!insert->parent);
  while (*current) {
    insert->parent = *current;
    if (comparator_(key, (*current)->key) == 0) {
      delete insert;
      return;
    }
    if (comparator_((*current)->key, key) < 0) {
      side = kRight;
      insert->ancestor_next = (*current)->ancestor_next;
      insert->ancestor_prev = *current;
      current = &((*current)->child[kRight]);
    } else {
      side = kLeft;
      insert->ancestor_next = *current;
      insert->ancestor_prev = (*current)->ancestor_prev;
      current = &((*current)->child[kLeft]);
    }
  }
  *current = insert;
  insert_guard.Unlock();

  port::RWLockWRGuard splay_guard(&rwlock_);
  // assert(current == insert);
  // assert(rwlock_.NumOfReaders() == 0);
  // assert(rwlock_.NumOfWriters() == 1);
  Splay(insert);
  insert->inserted = true;
  ++size_;
  // assert(root_ == insert);
  // assert(root_->parent == nullptr);
}

template <typename Key, typename Comparator>
inline bool SplayTree<Key, Comparator>::Contains(const Key &key) const {
  port::RWLockRDGuard _(&rwlock_);
  return UnlockFind(key);
}

template <typename Key, typename Comparator>
inline bool SplayTree<Key, Comparator>::Delete(const Key &key) {
  Node *n = Find(key);
  if (!n) {
    return false;
  }

  port::RWLockWRGuard delete_guard(&rwlock_);
  Splay(n);
  assert(root_ == n);
  assert(n->parent == nullptr);
  if (!n->child[kLeft]) {
    root_ = n->child[kRight];
    if (root_) {
      root_->parent = nullptr;
      assert(root_->ancestor_prev == n);
      root_->ancestor_prev = nullptr;
      assert(root_->ancestor_next == nullptr);
    }
    Collect(n);
  } else if (!n->child[kRight]) {
    root_ = n->child[kLeft];
    if (root_) {
      root_->parent = nullptr;
      assert(root_->ancestor_next == n);
      root_->ancestor_next = nullptr;
      assert(root_->ancestor_prev == nullptr);
    }
    Collect(n);
  } else {
    Node *c = UnlockSubMinimum(n->child[kRight]);
    if (c->parent == n) {
      // assert(n->child[kRight] == c);
      // assert(c->child[kLeft] == nullptr);
      c->child[kLeft] = n->child[kLeft];
      // assert(c->child[kLeft]);
      c->child[kLeft]->parent = c;
      assert(n->parent == nullptr);
      c->parent = nullptr;
      assert(c->ancestor_prev == n);
      c->ancestor_prev = nullptr;
      Collect(n);
    } else {
      // assert(c->parent->child[kLeft] == c);
      // assert(c->child[kLeft] == nullptr);
      c->parent->child[kLeft] = c->child[kRight];
      if (c->child[kRight]) {
        c->child[kRight]->parent = c->parent;
      }
      assert(n->parent == nullptr);
      c->parent = nullptr;

      c->child[kRight] = n->child[kRight];
      c->child[kRight]->parent = c;
      assert(c->child[kRight]->ancestor_prev == n);
      c->child[kRight]->ancestor_prev = c;
      // assert(c->child[kRight]->parent == c);

      c->child[kLeft] = n->child[kLeft];
      c->child[kLeft]->parent = c;
      assert(c->child[kLeft]->ancestor_next == n);
      c->child[kLeft]->ancestor_next = c;
      // assert(c->child[kLeft]->parent == c);

      Collect(n);
    }
    root_ = c;
  }
  --size_;
  return true;
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::First() {
  port::RWLockRDGuard _(&rwlock_);
  if (root_ && root_->child[kLeft]) {
    return UnlockSubMinimum(root_->child[kLeft]);
  } else {
    return root_;
  }
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::Last() {
  port::RWLockRDGuard _(&rwlock_);
  if (root_ && root_->child[kRight]) {
    return UnlockSubMaximum(root_->child[kRight]);
  } else {
    return root_;
  }
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::Next(Node *node) {
  port::RWLockRDGuard _(&rwlock_);
  return UnlockNext(node);
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::UnlockNext(Node *node) {
  if (node->child[kRight] && node->child[kRight]->inserted) {
    return UnlockSubMinimum(node->child[kRight]);
  } else if (node->ancestor_next && node->ancestor_next->inserted) {
    return node->ancestor_next;
  }
  return nullptr;
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::UnlockPrev(Node *node) {
  if (node->child[kLeft] && node->child[kLeft]->inserted) {
    return UnlockSubMaximum(node->child[kLeft]);
  } else if (node->ancestor_prev && node->ancestor_prev->inserted) {
    return node->ancestor_prev;
  }
  return nullptr;
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::Prev(Node *node) {
  port::RWLockRDGuard _(&rwlock_);
  return UnlockPrev(node);
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::FindGreaterOrEqual(const Key &key) {
  port::RWLockRDGuard _(&rwlock_);
  // assert(rwlock_.NumOfWriters() == 0);
  return UnlockFindGreaterOrEqual(key);
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::UnlockFindGreaterOrEqual(const Key &key) {
  Node *current = root_;
  Node *prev = nullptr;
  while (current && current->inserted) {
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
  if (current && current->inserted) {
    return current;
  }
  if (!prev) {
    return prev;
  }
  assert(prev->inserted);
  if (comparator_(key, prev->key) < 0) {
    return prev;
  }
  return UnlockNext(prev);
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::UnlockFind(const Key &key) const {
  Node *current = root_;
  while (current && current->inserted) {
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
SplayTree<Key, Comparator>::UnlockSubMinimum(
    SplayTree<Key, Comparator>::Node *n) {
  while (n->child[kLeft] && n->child[kLeft]->inserted) {
    n = n->child[kLeft];
  }
  return n;
}

template <typename Key, typename Comparator>
inline typename SplayTree<Key, Comparator>::Node *
SplayTree<Key, Comparator>::UnlockSubMaximum(
    SplayTree<Key, Comparator>::Node *n) {
  while (n->child[kRight] && n->child[kRight]->inserted) {
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
        // assert(n == n->parent->child[kRight]);
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
      // assert(n == n->parent->child[kRight] &&
      //     n->parent == n->parent->parent->child[kLeft]);
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
    // assert(root_ == n);
    root_ = c;
  } else if (n->parent->child[kLeft] == n) {
    n->parent->child[kLeft] = c;
  } else {
    // assert(n->parent->child[kRight] == n);
    n->parent->child[kRight] = c;
  }
  if (s == kLeft) {
    c->ancestor_next = n->ancestor_next;
    n->ancestor_prev = c;
  } else {
    n->ancestor_next = c;
    c->ancestor_prev = n->ancestor_prev;
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
inline const Key &SplayTree<Key, Comparator>::Iterator::key() const {
  return node_->key;
}

template <typename Key, typename Comparator>
inline bool SplayTree<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, typename Comparator>
inline void SplayTree<Key, Comparator>::Iterator::Prev() {
  node_ = tree_->Prev(node_);
}

template <typename Key, typename Comparator>
inline void SplayTree<Key, Comparator>::Iterator::Next() {
  node_ = tree_->Next(node_);
}

template <typename Key, typename Comparator>
inline void SplayTree<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = tree_->First();
}

template <typename Key, typename Comparator>
inline void SplayTree<Key, Comparator>::Iterator::Seek(const Key &target) {
  node_ = tree_->FindGreaterOrEqual(target);
}

template <typename Key, typename Comparator>
inline void SplayTree<Key, Comparator>::Iterator::SeekToLast() {
  node_ = tree_->Last();
}

}  // namespace leveldb
#endif
