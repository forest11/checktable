package main

import (
	"sort"
	"sync"
)


// Set 集合
type Set struct {
	sync.RWMutex
	m map[string]bool
}


// NewSet 新建集合对象
func NewSet(items ...string) *Set {
	s := &Set{
		m: make(map[string]bool, len(items)),
	}
	s.Add(items...)
	return s
}


// Add 添加元素
func (s *Set) Add(items ...string) {
	s.Lock()
	defer s.Unlock()
	for _, v := range items {
		s.m[v] = true
	}
}


//Remove 删除元素
func (s *Set) Remove(items ...string) {
	s.Lock()
	defer s.Unlock()
	for _, v := range items {
		delete(s.m, v)
	}
}


//Has 判断元素是否存在
func (s *Set) Has(items ...string) bool {
	s.RLock()
	defer s.RUnlock()
	for _, v := range items {
		if _, ok := s.m[v]; !ok {
			return false
		}
	}
	return true
}

// Count 元素个数
func (s *Set) Count() int {
	return len(s.m)
}

//Clear 清空集合
func (s *Set) Clear() {
	s.Lock()
	defer s.Unlock()
	s.m = map[string]bool{}
}

//Empty 空集合判断
func (s *Set) Empty() bool {
	return len(s.m) == 0
}

//List 无序列表
func (s *Set) List() []string {
	s.RLock()
	defer s.RUnlock()
	list := make([]string, 0, len(s.m))
	for item := range s.m {
		list = append(list, item)
	}
	return list
}

//SortList 排序列表
func (s *Set) SortList() []string {
	s.RLock()
	defer s.RUnlock()
	list := make([]string, 0, len(s.m))
	for item := range s.m {
		list = append(list, item)
	}
	sort.Strings(list)
	return list
}

//Union 并集
func (s *Set) Union(sets ...*Set) *Set {
	r := NewSet(s.List()...)
	for _, set := range sets {
		for e := range set.m {
			r.m[e] = true
		}
	}
	return r
}

//Minus 差集
func (s *Set) Minus(sets ...*Set) *Set {
	r := NewSet(s.List()...)
	for _, set := range sets {
		for e := range set.m {
			if _, ok := s.m[e]; ok {
				delete(r.m, e)
			}
		}
	}
	return r
}

//Intersect 交集
func (s *Set) Intersect(sets ...*Set) *Set {
	r := NewSet(s.List()...)
	for _, set := range sets {
		for e := range s.m {
			if _, ok := set.m[e]; !ok {
				delete(r.m, e)
			}
		}
	}
	return r
}

//Complement 补集
func (s *Set) Complement(full *Set) *Set {
	r := NewSet()
	for e := range full.m {
		if _, ok := s.m[e]; !ok {
			r.Add(e)
		}
	}
	return r
}