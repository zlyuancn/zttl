/*
-------------------------------------------------
   Author :       zlyuan
   date：         2019/12/14
   Description :  ttl管理模块, 可用于为不支持ttl的缓存模拟ttl功能
-------------------------------------------------
*/

package zttl

import (
    "hash/crc32"
    "sync"
    "time"
)

type entry struct {
    deadline int64
    ttl      int64
}

type TimeToLive struct {
    ttl   int64
    shard uint32
    mxs   []*sync.Mutex
    mm    []map[string]*entry
}

// 创建一个ttl控制器
// shard表示分片数量, 它将key做hash取模分配到指定的分片上
// ttl表示默认ttl
func New(shard uint32, ttl int64) *TimeToLive {
    if shard < 1 {
        shard = 1
    }

    mxs := make([]*sync.Mutex, shard)
    mm := make([]map[string]*entry, shard)

    for i := uint32(0); i < shard; i++ {
        mxs[i] = new(sync.Mutex)
        mm[i] = make(map[string]*entry)
    }

    return &TimeToLive{
        ttl:   ttl,
        shard: shard,
        mxs:   mxs,
        mm:    mm,
    }
}

func (m *TimeToLive) getMM(key string) (*sync.Mutex, map[string]*entry) {
    hash := crc32.ChecksumIEEE([]byte(key))
    i := hash % m.shard
    return m.mxs[i], m.mm[i]
}

// 使用默认TTL添加一个key, 如果key已存在会刷新ttl
func (m *TimeToLive) AddDefault(key string) {
    m.Add(key, m.ttl)
}

// 添加key, 如果key已存在会刷新ttl
func (m *TimeToLive) Add(key string, ttl int64) {
    mx, entrys := m.getMM(key)
    mx.Lock()

    e, ok := entrys[key]
    if ok {
        e.ttl = ttl
        e.deadline = time.Now().UnixNano() + ttl
    } else {
        e = &entry{
            ttl:      ttl,
            deadline: time.Now().UnixNano() + ttl,
        }
        entrys[key] = e
    }

    mx.Unlock()
}

// 获取
func (m *TimeToLive) Get(key string) bool {
    mx, entrys := m.getMM(key)
    mx.Lock()

    e, ok := entrys[key]
    if ok && time.Now().UnixNano() >= e.deadline {
        ok = false
        delete(entrys, key)
    }

    mx.Unlock()
    return ok
}

// 获取并刷新TTL
func (m *TimeToLive) GetAndRefresh(key string) bool {
    return m.GetAndSetTTL(key, 0)
}

// 获取并设置TTL
func (m *TimeToLive) GetAndSetTTL(key string, ttl int64) bool {
    mx, entrys := m.getMM(key)
    mx.Lock()

    e, ok := entrys[key]
    if ok {
        now := time.Now().UnixNano()
        if now >= e.deadline {
            ok = false
            delete(entrys, key)
        } else {
            if ttl == 0 {
                ttl = e.ttl
            }
            e.deadline = now + ttl
        }
    }

    mx.Unlock()
    return ok
}
