package common

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Dao interface {
	InsertClient(name string) Client
	InsertCustomer(code, firstName string, lastName string, email string, client Client) Customer
	InsertProduct(name string) Product
	UpdateCustomerName(customer Customer, newFullName string)
	UpdateProductName(product Product, newName string)
	UpdateCustomerEmailAndLinkToProduct(customer Customer, newEmail string, product Product)
	UpdateClientName(client Client, newName string)
	DeleteClient(client Client)
	DeleteCustomer(customer Customer)
	DeleteAllCustomers()
	DeleteAllProducts()
	DeleteAllClients()
	PrintDatabaseState()
	Shutdown()
}

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
	Products     []Product `gorm:"many2many:customer_product;"`
	ClientId     int64
	Code         string
	FirstName    string
	MiddleName   string
	LastName     string
	EmailAddress string
}

type Product struct {
	UpdatableRecord
	Name   string
	Active bool
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

func SplitFullName(fullName string) (string, string, error) {
	var err error
	names := strings.Split(fullName, " ")
	if len(names) != 2 {
		err = errors.New("Invalid full name")
	}
	return names[0], names[1], err
}
