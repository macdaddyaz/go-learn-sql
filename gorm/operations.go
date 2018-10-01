package gorm

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	. "go-learn-sql/common"
	"log"
	"strings"
	"time"
)

type gormDao struct {
	*gorm.DB
}

//noinspection GoExportedFuncWithUnexportedType
func Init() gormDao {
	db, err := gorm.Open("postgres", DefaultConnectionString())
	if err != nil {
		log.Fatal(err)
	}
	db.
		// LogMode(true).
		SingularTable(true)
	return gormDao{db}
}

func (dao gormDao) Shutdown() {
	err := dao.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func (dao gormDao) PrintDatabaseState() {
	dao.printClients()
	dao.printProducts()
	dao.printCustomers()
}
func (dao gormDao) printClients() {
	log.Printf("*** %-15s ***", "Clients")
	var clients []Client
	result := dao.Order("id").Find(&clients)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	log.Printf("%-3s | %-40s | %s | %-20s | %-20s", "ID", "Name", "Active", "Created At", "Updated At")
	log.Println(strings.Repeat("-", 101))
	for _, client := range clients {
		log.Printf("%-3d | %-40s | %-6t | %20s | %20s",
			client.Id,
			client.Name,
			client.Active,
			client.CreatedAt.Format(time.RFC822),
			client.UpdatedAt.Format(time.RFC822))
	}
	log.Printf("Total: %d row(s)", len(clients))
}

func (dao gormDao) printProducts() {
	log.Printf("*** %-15s ***", "Products")
	var products []Product
	result := dao.Order("id").Find(&products)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	log.Printf("%-3s | %-40s | %s | %-20s | %-20s", "ID", "Name", "Active", "Created At", "Updated At")
	log.Println(strings.Repeat("-", 101))
	for _, product := range products {
		log.Printf("%-3d | %-40s | %-6t | %20s | %20s",
			product.Id,
			product.Name,
			product.Active,
			product.CreatedAt.Format(time.RFC822),
			product.UpdatedAt.Format(time.RFC822))
	}
	log.Printf("Total: %d row(s)", len(products))
}

func (dao gormDao) printCustomers() {
	log.Printf("*** %-15s ***", "Customers")
	log.Printf("%-3s | %-10s | %-20s | %-20s | %-40s | %-40s | %20s | %20s",
		"ID",
		"Code",
		"First Name",
		"Last Name",
		"Email",
		"Client",
		"Created At",
		"Updated At")
	log.Println(strings.Repeat("-", 194))
	var customers []Customer
	result := dao.Preload("Products").Order("id").Find(&customers)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	for _, customer := range customers {
		log.Printf("%-3d | %-10s | %-20s | %-20s | %-40s | %-40s | %20s | %20s",
			customer.Id,
			customer.Code,
			customer.FirstName,
			customer.LastName,
			customer.EmailAddress,
			customer.Client.Name,
			customer.CreatedAt.Format(time.RFC822),
			customer.UpdatedAt.Format(time.RFC822))
	}
	log.Printf("Total: %d row(s)", len(customers))
	// Customer/Product relationship
	log.Printf("*** %-15s ***", "Customer/Products")
	log.Printf("%-10s | %-20s | %-20s | %-40s", "Code", "First Name", "Last Name", "Product")
	log.Println(strings.Repeat("-", 99))
	var rowCount int
	for _, customer := range customers {
		for _, product := range customer.Products {
			rowCount++
			log.Printf("%-10s | %-20s | %-20s | %-40s", customer.Code, customer.FirstName, customer.LastName, product.Name)
		}
	}
	log.Printf("Total: %d row(s)", rowCount)
}

func (dao gormDao) InsertClient(name string) Client {
	log.Println("Insert client", name)
	client := Client{
		Name:   name,
		Active: true,
	}
	result := dao.Create(&client)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	return client
}

func (dao gormDao) InsertCustomer(code, firstName string, lastName string, email string, client Client) Customer {
	log.Println("Insert customer", firstName, lastName)
	customer := Customer{
		Code:         code,
		FirstName:    firstName,
		LastName:     lastName,
		EmailAddress: email,
		Client:       client,
	}
	result := dao.Create(&customer)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	return customer
}

func (dao gormDao) InsertProduct(name string) Product {
	log.Println("Insert product", name)
	product := Product{
		Name: name,
	}
	result := dao.Create(&product)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	return product
}

func (dao gormDao) UpdateCustomerName(customer Customer, newFullName string) {
	log.Println("Update customer", customer.Id, "name to", newFullName)
	newFirstName, newLastName, err := SplitFullName(newFullName)
	if err != nil {
		log.Fatal(err)
	}
	customer.FirstName = newFirstName
	customer.LastName = newLastName
	result := dao.Save(&customer)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	logAffectedRows("Update customer name", result)
}

func (dao gormDao) UpdateProductName(product Product, newName string) {
	log.Println("Update product", product.Id, "name to", newName)
	product.Name = newName
	result := dao.Save(&product)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	logAffectedRows("Update product name", result)
}

func (dao gormDao) UpdateCustomerEmailAndLinkToProduct(customer Customer, newEmail string, product Product) {
	log.Println("Update customer", customer.Id, "email address to", newEmail)
	customer.EmailAddress = newEmail
	log.Println("Link product", product.Id, "to customer", customer.Id)
	customer.Products = append(customer.Products, product)
	result := dao.Save(&customer)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	logAffectedRows("Update customer email and link to product", result)
}

func (dao gormDao) DeleteClient(client Client) {
	log.Println("Delete client", client.Id)
	result := dao.Delete(&client)
	if result.Error == nil {
		log.Fatal("Delete client was not blocked by DB constraints")
	}
	// log.Println(result.Error)
	log.Println("Delete client was blocked by DB contraints, as expected")
}

func (dao gormDao) UpdateClientName(client Client, newName string) {
	log.Println("Update client", client.Id, "name to", newName)
	client.Name = newName
	result := dao.Save(&client)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	logAffectedRows("Update client name", result)
}

func (dao gormDao) DeleteCustomer(customer Customer) {
	log.Println("Delete customer", customer.Id)
	result := dao.Delete(&customer)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	logAffectedRows("Delete customer", result)
}

func (dao gormDao) DeleteAllCustomers() {
	log.Println("Delete all customers")
	result := dao.Delete(Customer{})
	logAffectedRows("Delete all customers", result)
}

func (dao gormDao) DeleteAllProducts() {
	log.Println("Delete all products")
	result := dao.Delete(Product{})
	logAffectedRows("Delete all products", result)
}

func (dao gormDao) DeleteAllClients() {
	log.Println("Delete all clients")
	result := dao.Delete(Client{})
	logAffectedRows("Delete all clients", result)
}

func logAffectedRows(prefix string, db *gorm.DB) {
	log.Printf("%-20s: %d row(s) affected", prefix, db.RowsAffected)
}
