package common

import "fmt"

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
	Id int64
}

type Customer struct {
	DataRecord
}

type Client struct {
	DataRecord
}

type Product struct {
	DataRecord
}

func NewCustomer(id int64) Customer {
	return Customer{DataRecord{id}}
}

func NewClient(id int64) Client {
	return Client{DataRecord{id}}
}

func NewProduct(id int64) Product {
	return Product{DataRecord{id}}
}
