## 自定义表名
### 模型中自定义表名设置
```go
type Tabler interface {
TableName() string
}

// TableName 会将 User 的表名重写为 `user_new_name`
func (User) TableName() string {
return "user_new_name"
}
```

## 列名设置
```go
type User struct {
  ID        uint      // 列名是 `id`
  Name      string    // 列名是 `name`
  Birthday  time.Time // 列名是 `birthday`
  CreatedAt time.Time // 列名是 `created_at`
}

type User struct {
ID int64     `gorm:"column:user_id"`         // 将列名设为 `user_id`
Name string `gorm:"column:user_name"` // 将列名设为 `user_name`
Birthday      time.Time     `gorm:"column:user_birthday"` // 将列名设为 `user_birthday`
}
```

## 数据行创建和更新时间设置
```go
type User struct {
  CreatedAt time.Time // 在创建时，如果该字段值为零值，则使用当前时间填充
  UpdatedAt int       // 在创建时该字段值为零值或者在更新时，使用当前时间戳秒数填充
  Updated   int64 `gorm:"autoUpdateTime:nano"` // 使用时间戳填纳秒数充更新时间
  Updated   int64 `gorm:"autoUpdateTime:milli"` // 使用时间戳毫秒数填充更新时间
  Created   int64 `gorm:"autoCreateTime"`      // 使用时间戳秒数填充创建时间
}
```

## 结构体字段嵌入
```go
type Goods struct {
    Name  string
    Price 
}


```

