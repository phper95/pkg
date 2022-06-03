package db

import "gorm.io/gorm"

type db struct {
	*gorm.DB
}
type BaseQueryBuilder struct {
	Fields []string
	Order  []string
	Where  []struct {
		Prefix string
		Value  interface{}
	}
	Limit  int
	Offset int
}

func (db *db) buildQuery(queryBuilder *BaseQueryBuilder) *gorm.DB {
	for _, where := range queryBuilder.Where {
		db.Where(where.Prefix, where.Value)
	}
	for _, order := range queryBuilder.Order {
		db.Order(order)
	}
	db.Limit(queryBuilder.Limit).Offset(queryBuilder.Offset)
	return db.DB
}
func (qb *authQueryBuilder) Delete(db *gorm.DB) (err error) {
	for _, where := range qb.where {
		db = db.Where(where.prefix, where.value)
	}

	if err = db.Delete(&Auth{}).Error; err != nil {
		return errors.Wrap(err, "delete err")
	}
	return nil
}
