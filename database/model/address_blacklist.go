package model

type AddressBlacklist struct {
	Address string `gorm:"type:varchar(42);primaryKey;not null"`
	Reason  string `gorm:"type:text"`
}

func (AddressBlacklist) TableName() string {
	return "address_blacklist"
}
