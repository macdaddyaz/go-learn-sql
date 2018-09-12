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
	db *gorm.DB
}

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
	err := dao.db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func (dao gormDao) PrintDatabaseState() {
	printClients(dao)
	printProducts(dao)
	printCustomers(dao)
}
func printClients(dao gormDao) {
	log.Printf("*** %-15s ***", "Clients")
	var clients []Client
	result := dao.db.Find(&clients)
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

func printProducts(dao gormDao) {
	log.Printf("*** %-15s ***", "Products")
	var products []Product
	result := dao.db.Find(&products)
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

func printCustomers(dao gormDao) {
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
	result := dao.db.Find(&customers)
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
	// TODO Load product relationship and print
}

func (dao gormDao) InsertClient(name string) Client {
	log.Println("Insert client", name)
	client := Client{
		Name:   name,
		Active: true,
	}
	result := dao.db.Create(&client)
	if result.Error != nil {
		log.Fatal(result.Error)
	}
	return client
}

func (dao gormDao) InsertCustomer(code, firstName string, lastName string, email string, client Client) Customer {
	log.Println("Insert customer", firstName, lastName)
	return Customer{}
}

func (dao gormDao) InsertProduct(name string) Product {
	log.Println("Insert product", name)
	return Product{}
}

func (dao gormDao) UpdateCustomerName(customer Customer, newFullName string) {
	log.Println("Update customer", customer.Id, "name to", newFullName)
	// newFirstName, newLastName, err := SplitFullName(newFullName)
	// logAffectedRows("Update customer name", res)
}

func (dao gormDao) UpdateProductName(product Product, newName string) {
	log.Println("Update product", product.Id, "name to", newName)
	// logAffectedRows("Update product name", res)
}

func (dao gormDao) UpdateCustomerEmailAndLinkToProduct(customer Customer, newEmail string, product Product) {
	log.Println("Update customer", customer.Id, "email address to", newEmail)
	// logAffectedRows("Update customer email", res)
	log.Println("Link product", product.Id, "to customer", customer.Id)
	// logAffectedRows("Link customer to product", res)
}

func (dao gormDao) DeleteClient(client Client) {
	log.Println("Delete client", client.Id)
	log.Println("Delete client was blocked by DB contraints, as expected")
}

func (dao gormDao) UpdateClientName(client Client, newName string) {
	log.Println("Update client", client.Id, "name to", newName)
	// logAffectedRows("Update client name", res)
}

func (dao gormDao) DeleteCustomer(customer Customer) {
	log.Println("Delete customer", customer.Id)
	// logAffectedRows("Delete customer", res)
}

func (dao gormDao) DeleteAllCustomers() {
	log.Println("Delete all customers")
	// logAffectedRows("Delete all customers", res)
}

func (dao gormDao) DeleteAllProducts() {
	log.Println("Delete all products")
	// logAffectedRows("Delete all products", res)
}

func (dao gormDao) DeleteAllClients() {
	log.Println("Delete all clients")
	dao.db.Delete(Client{})
	// logAffectedRows("Delete all clients", res)
}

func logAffectedRows(prefix string, dao gormDao) {
	log.Printf("%-20s: %d row(s) affected", prefix, dao.db.RowsAffected)
}
