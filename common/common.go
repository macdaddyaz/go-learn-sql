package common

import "fmt"

type DbParams struct {
	host     string
	port     uint16
	username string
	password string
	database string
	sslmode  string
}

var defaultParams DbParams = DbParams{
	host:     "localhost",
	port:     10032,
	username: "postgres",
	password: "lGzLc4okX9Gz",
	database: "cs_arch_playground",
	sslmode:  "disable",
}

func DefaultConnectionString() string {
	return ConnectionString(defaultParams)
}

func ConnectionString(params DbParams) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		params.host,
		params.port,
		params.username,
		params.password,
		params.database,
		params.sslmode)
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
