package common

import (
	"fmt"
	"time"
)

type DbParams struct {
	Host     string
	Port     uint16
	Username string
	Password string
	Database string
	SslMode  string
}

var DefaultParams DbParams = DbParams{
	Host:     "localhost",
	Port:     10032,
	Username: "postgres",
	Password: "lGzLc4okX9Gz",
	Database: "cs_arch_playground",
	SslMode:  "disable",
}

func DefaultConnectionString() string {
	return ConnectionString(DefaultParams)
}

func ConnectionString(params DbParams) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		params.Host,
		params.Port,
		params.Username,
		params.Password,
		params.Database,
		params.SslMode)
}

func ConnectionUrl(params DbParams) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		params.Username,
		params.Password,
		params.Host,
		params.Port,
		params.Database,
		params.SslMode)
}

type DataRecord struct {
	Id        int64 `gorm:"primary_key"`
	CreatedAt time.Time
}

type UpdatableRecord struct {
	DataRecord
	UpdatedAt time.Time
}

type Client struct {
	UpdatableRecord
	Customers []Customer
	Name      string
	Active    bool
}

type Customer struct {
	UpdatableRecord
	Client       Client
	ClientId     int64
	Code         string
	FirstName    string
	MiddleName   string
	LastName     string
	EmailAddress string
}

type Product struct {
	UpdatableRecord
}

type CustomerProduct struct {
	DataRecord
}

func NewCustomer(id int64) Customer {
	return Customer{UpdatableRecord: UpdatableRecord{DataRecord: DataRecord{Id: id}}}
}

func NewClient(id int64) Client {
	return Client{UpdatableRecord: UpdatableRecord{DataRecord: DataRecord{Id: id}}}
}

func NewProduct(id int64) Product {
	return Product{UpdatableRecord: UpdatableRecord{DataRecord: DataRecord{Id: id}}}
}
