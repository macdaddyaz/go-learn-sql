package main

// Experiment with database access using only Go's database/sql package
// Using documentation from http://go-database-sql.org/

// Seed data
//   1. Insert 2 clients
//   2. Insert 8 customers
//   3. Insert 3 products
//   4. Link all customers to 1 or 2 random products
// Find and update some data
//   1. Find customer and update name
//   2. Find product and update name
//   3. Find customer, update email, and create link to new product (transaction)
//   4. Find client and delete it (should produce error)
//   5. Find client and update name
//   6. Find customer and delete it
// Cleanup data
//   1. Delete customers (should delete customer/product links too)
//   2. Delete products
//   3. Delete clients

import (
	"database/sql"
	"errors"
	_ "github.com/lib/pq"
	. "go-learn-sql/common"
	"log"
	"strings"
)

var db *sql.DB

func main() {
	db = openDb()
	defer db.Close()
	log.Print(db)

	clients := [2]Client{
		insertClient("Los Angeles Lakers"),
		insertClient("Boston Celtics"),
	}

	customers := [8]Customer{
		insertCustomer("123", "Kobe", "Bryant", "kbryant8@lakers.com", clients[0]),
		insertCustomer("234", "Shaquille", "O'Neal", "soneal@lakers.com", clients[0]),
		insertCustomer("345", "Magic", "Johnson", "mjohnson@lakers.com", clients[0]),
		insertCustomer("456", "Kareem", "Abdul-Jabbar", "kabduljabbar@lakers.com", clients[0]),
		insertCustomer("567", "Jerry", "West", "jwest@lakers.com", clients[0]),
		insertCustomer("678", "Bill", "Russell", "brussel@celtics.com", clients[1]),
		insertCustomer("789", "Larry", "Bird", "lbird@celtics.com", clients[1]),
		insertCustomer("890", "Paul", "Pierce", "ppierce@celtics.com", clients[1]),
	}

	products := [3]Product{
		insertProduct("Super Personal Resolution Service"),
		insertProduct("Fantastic Identity Monitoring"),
		insertProduct("Watching Some Other Stuff"),
	}

	updateCustomerName(customers[3], "Lew Alcindor")
	updateProductName(products[2], "Stupendous Cyber Monitoring")
	updateCustomerEmailAndLinkToProduct(customers[4], "jwest@clippers.com", products[1])
	deleteClient(clients[1])
	updateClientName(clients[1], "Evil Empire")
	deleteCustomer(customers[7])

	deleteAllCustomers()
	deleteAllProducts()
	deleteAllClients()
}

func openDb() *sql.DB {
	connStr := DefaultConnectionString()
	handle, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	err = handle.Ping()
	if err != nil {
		handle.Close()
		log.Fatal(err)
	}
	return handle
}

func insertClient(name string) Client {
	log.Println("Insert client", name)
	var id int64
	err := db.QueryRow(
		`INSERT INTO client (name, active)
		VALUES ($1, true)
		RETURNING id`, name).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	return NewClient(id)
}

func insertCustomer(code, firstName string, lastName string, email string, client Client) Customer {
	log.Println("Insert customer", firstName, lastName)
	var id int64
	err := db.QueryRow(
		`INSERT INTO customer (code, first_name, last_name, email_address, client_id)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id`, code, firstName, lastName, email, client.Id).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	return NewCustomer(id)
}

func insertProduct(name string) Product {
	log.Println("Insert product", name)
	var id int64
	err := db.QueryRow(
		`INSERT INTO product (name, active)
		VALUES ($1, true)
		RETURNING id`, name).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	return NewProduct(id)
}

func updateCustomerName(customer Customer, newFullName string) {
	log.Println("Update customer", customer.Id, "name to", newFullName)
	newFirstName, newLastName, err := splitFullName(newFullName)
	if err != nil {
		log.Fatal(err)
	}
	res, err := db.Exec(
		`UPDATE customer
		SET first_name = $2
		  , last_name = $3
		WHERE id = $1`, customer.Id, newFirstName, newLastName)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Update customer name", res)
}

func updateProductName(product Product, newName string) {
	log.Println("Update product", product.Id, "name to", newName)
	res, err := db.Exec(
		`UPDATE product
		SET name = $2
		WHERE id = $1`, product.Id, newName)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Update product name", res)
}

func updateCustomerEmailAndLinkToProduct(customer Customer, newEmail string, product Product) {
	log.Println("Update customer", customer.Id, "email address to", newEmail)
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	res, err := tx.Exec(
		`UPDATE customer
			SET email_address = $2
			WHERE id = $1`, customer.Id, newEmail)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	logAffectedRows("Update customer email", res)
	log.Println("Link product", product.Id, "to customer", customer.Id)
	res, err = tx.Exec(
		`INSERT INTO customer_product (customer_id, product_id)
		VALUES ($1, $2)`, customer.Id, product.Id)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	logAffectedRows("Link customer to product", res)
	tx.Commit()
}

func deleteClient(client Client) {
	log.Println("Delete client", client.Id)
	_, err := db.Exec(
		`DELETE FROM client
			WHERE id = $1`, client.Id)
	if err == nil {
		log.Fatal("Delete client was not blocked by DB constraints")
	}
	log.Println("Delete client was blocked by DB contraints, as expected")
}

func updateClientName(client Client, newName string) {
	log.Println("Update client", client.Id, "name to", newName)
	res, err := db.Exec(
		`UPDATE client
			SET name = $2
			WHERE id = $1`, client.Id, newName)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Update client name", res)
}

func deleteCustomer(customer Customer) {
	log.Println("Delete customer", customer.Id)
	res, err := db.Exec(
		`DELETE FROM customer
			WHERE id = $1`, customer.Id)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Delete customer", res)
}

func deleteAllCustomers() {
	log.Println("Delete all customers")
	res, err := db.Exec(`DELETE FROM customer`)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Delete all customers", res)
}

func deleteAllProducts() {
	log.Println("Delete all products")
	res, err := db.Exec(`DELETE FROM product`)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Delete all products", res)
}

func deleteAllClients() {
	log.Println("Delete all clients")
	res, err := db.Exec(`DELETE FROM client`)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Delete all clients", res)
}

func splitFullName(fullName string) (string, string, error) {
	var err error
	names := strings.Split(fullName, " ")
	if len(names) != 2 {
		err = errors.New("Invalid full name")
	}
	return names[0], names[1], err
}

func logAffectedRows(prefix string, res sql.Result) {
	rowsAffected, _ := res.RowsAffected()
	log.Printf("%-20s: %d row(s) affected", prefix, rowsAffected)
}
