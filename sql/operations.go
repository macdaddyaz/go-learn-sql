package sql

import (
	"database/sql"
	"errors"
	_ "github.com/lib/pq"
	. "go-learn-sql/common"
	"log"
	"strings"
	"time"
)

var Data *sql.DB

func OpenDb() *sql.DB {
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

func PrintDatabaseState() {
	printClients()
	printProducts()
	printCustomers()
	printCustomerProducts()
}

func printClients() {
	log.Printf("*** %-15s ***", "Clients")
	clients, err := Data.Query("SELECT id, name, active, created_at, updated_at FROM client ORDER BY id")
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
	products, err := Data.Query("SELECT id, name, active, created_at, updated_at FROM product ORDER BY id")
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
	customers, err := Data.Query(`
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
	customerProducts, err := Data.Query(`
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

func InsertClient(name string) Client {
	log.Println("Insert client", name)
	var id int64
	err := Data.QueryRow(
		`INSERT INTO client (name, active)
		VALUES ($1, true)
		RETURNING id`, name).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	return NewClient(id)
}

func InsertCustomer(code, firstName string, lastName string, email string, client Client) Customer {
	log.Println("Insert customer", firstName, lastName)
	var id int64
	err := Data.QueryRow(
		`INSERT INTO customer (code, first_name, last_name, email_address, client_id)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id`, code, firstName, lastName, email, client.Id).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	return NewCustomer(id)
}

func InsertProduct(name string) Product {
	log.Println("Insert product", name)
	var id int64
	err := Data.QueryRow(
		`INSERT INTO product (name, active)
		VALUES ($1, true)
		RETURNING id`, name).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	return NewProduct(id)
}

func UpdateCustomerName(customer Customer, newFullName string) {
	log.Println("Update customer", customer.Id, "name to", newFullName)
	newFirstName, newLastName, err := splitFullName(newFullName)
	if err != nil {
		log.Fatal(err)
	}
	res, err := Data.Exec(
		`UPDATE customer
		SET first_name = $2
		  , last_name = $3
		WHERE id = $1`, customer.Id, newFirstName, newLastName)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Update customer name", res)
}

func UpdateProductName(product Product, newName string) {
	log.Println("Update product", product.Id, "name to", newName)
	res, err := Data.Exec(
		`UPDATE product
		SET name = $2
		WHERE id = $1`, product.Id, newName)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Update product name", res)
}

func UpdateCustomerEmailAndLinkToProduct(customer Customer, newEmail string, product Product) {
	log.Println("Update customer", customer.Id, "email address to", newEmail)
	tx, err := Data.Begin()
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

func DeleteClient(client Client) {
	log.Println("Delete client", client.Id)
	_, err := Data.Exec(
		`DELETE FROM client
			WHERE id = $1`, client.Id)
	if err == nil {
		log.Fatal("Delete client was not blocked by DB constraints")
	}
	log.Println("Delete client was blocked by DB contraints, as expected")
}

func UpdateClientName(client Client, newName string) {
	log.Println("Update client", client.Id, "name to", newName)
	res, err := Data.Exec(
		`UPDATE client
			SET name = $2
			WHERE id = $1`, client.Id, newName)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Update client name", res)
}

func DeleteCustomer(customer Customer) {
	log.Println("Delete customer", customer.Id)
	res, err := Data.Exec(
		`DELETE FROM customer
			WHERE id = $1`, customer.Id)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Delete customer", res)
}

func DeleteAllCustomers() {
	log.Println("Delete all customers")
	res, err := Data.Exec(`DELETE FROM customer`)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Delete all customers", res)
}

func DeleteAllProducts() {
	log.Println("Delete all products")
	res, err := Data.Exec(`DELETE FROM product`)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Delete all products", res)
}

func DeleteAllClients() {
	log.Println("Delete all clients")
	res, err := Data.Exec(`DELETE FROM client`)
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
