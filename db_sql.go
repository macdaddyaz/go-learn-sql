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
	"time"
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
	printDatabaseState()

	updateCustomerName(customers[3], "Lew Alcindor")
	updateProductName(products[2], "Stupendous Cyber Monitoring")
	updateCustomerEmailAndLinkToProduct(customers[4], "jwest@clippers.com", products[1])
	deleteClient(clients[1])
	updateClientName(clients[1], "Evil Empire")
	deleteCustomer(customers[7])
	printDatabaseState()

	deleteAllCustomers()
	deleteAllProducts()
	deleteAllClients()
	printDatabaseState()
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

func printDatabaseState() {
	printClients()
	printProducts()
	printCustomers()
	printCustomerProducts()
}

func printClients() {
	log.Printf("*** %-15s ***", "Clients")
	clients, err := db.Query("SELECT id, name, active, created_at, updated_at FROM client ORDER BY id")
	if err != nil {
		log.Fatal(err)
	}
	defer clients.Close()
	var (
		id        int64
		name      string
		active    bool
		createdAt time.Time
		updatedAt time.Time
	)
	log.Printf("%-3s | %-40s | %s | %-20s | %-20s", "ID", "Name", "Active", "Created At", "Updated At")
	log.Println(strings.Repeat("-", 101))
	rowCount := 0
	for ; clients.Next(); rowCount++ {
		err = clients.Scan(&id, &name, &active, &createdAt, &updatedAt)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%-3d | %-40s | %-6t | %20s | %20s", id, name, active, createdAt.Format(time.RFC822), updatedAt.Format(time.RFC822))
	}
	log.Printf("Total: %d row(s)", rowCount)
}

func printProducts() {
	log.Printf("*** %-15s ***", "Products")
	products, err := db.Query("SELECT id, name, active, created_at, updated_at FROM product ORDER BY id")
	if err != nil {
		log.Fatal(err)
	}
	defer products.Close()
	var (
		id        int64
		name      string
		active    bool
		createdAt time.Time
		updatedAt time.Time
	)
	log.Printf("%-3s | %-40s | %s | %-20s | %-20s", "ID", "Name", "Active", "Created At", "Updated At")
	log.Println(strings.Repeat("-", 101))
	rowCount := 0
	for ; products.Next(); rowCount++ {
		err = products.Scan(&id, &name, &active, &createdAt, &updatedAt)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%-3d | %-40s | %-6t | %20s | %20s", id, name, active, createdAt.Format(time.RFC822), updatedAt.Format(time.RFC822))
	}
	log.Printf("Total: %d row(s)", rowCount)
}

func printCustomers() {
	log.Printf("*** %-15s ***", "Customers")
	customers, err := db.Query(`
		SELECT c.id, c.code, c.first_name, c.last_name, c.email_address, cl.name, c.created_at, c.updated_at
		FROM customer c
		JOIN client cl ON cl.id = c.client_id
		ORDER BY c.id`)
	if err != nil {
		log.Fatal(err)
	}
	defer customers.Close()
	var (
		id           int64
		code         string
		firstName    string
		lastName     string
		emailAddress string
		clientName   string
		createdAt    time.Time
		updatedAt    time.Time
	)
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
	rowCount := 0
	for ; customers.Next(); rowCount++ {
		err = customers.Scan(&id, &code, &firstName, &lastName, &emailAddress, &clientName, &createdAt, &updatedAt)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%-3d | %-10s | %-20s | %-20s | %-40s | %-40s | %20s | %20s",
			id, code, firstName, lastName, emailAddress, clientName, createdAt.Format(time.RFC822), updatedAt.Format(time.RFC822))
	}
	log.Printf("Total: %d row(s)", rowCount)
}

func printCustomerProducts() {
	log.Printf("*** %-15s ***", "Customer/Products")
	customerProducts, err := db.Query(`
		SELECT c.code, c.first_name, c.last_name, p.name
		FROM customer c
		INNER JOIN customer_product cp ON c.id = cp.customer_id
		INNER JOIN product p ON cp.product_id = p.id
		ORDER BY c.last_name`)
	if err != nil {
		log.Fatal(err)
	}
	defer customerProducts.Close()
	var (
		code      string
		firstName string
		lastName  string
		product   string
	)
	log.Printf("%-10s | %-20s | %-20s | %-40s", "Code", "First Name", "Last Name", "Product")
	log.Println(strings.Repeat("-", 99))
	rowCount := 0
	for ; customerProducts.Next(); rowCount++ {
		err = customerProducts.Scan(&code, &firstName, &lastName, &product)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%-10s | %-20s | %-20s | %-40s", code, firstName, lastName, product)
	}
	log.Printf("Total: %d row(s)", rowCount)
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
