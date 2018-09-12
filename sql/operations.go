package sql

import (
	"database/sql"
	_ "github.com/lib/pq"
	. "go-learn-sql/common"
	"log"
	"strings"
	"time"
)

type sqlDao struct {
	db *sql.DB
}

func Init() sqlDao {
	connStr := DefaultConnectionString()
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		db.Close()
		log.Fatal(err)
	}
	return sqlDao{db}
}

func (dao sqlDao) Shutdown() {
	err := dao.db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func (dao sqlDao) PrintDatabaseState() {
	printClients(dao)
	printProducts(dao)
	printCustomers(dao)
	printCustomerProducts(dao)
}

func printClients(dao sqlDao) {
	log.Printf("*** %-15s ***", "Clients")
	clients, err := dao.db.Query("SELECT id, name, active, created_at, updated_at FROM client ORDER BY id")
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

func printProducts(dao sqlDao) {
	log.Printf("*** %-15s ***", "Products")
	products, err := dao.db.Query("SELECT id, name, active, created_at, updated_at FROM product ORDER BY id")
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

func printCustomers(dao sqlDao) {
	log.Printf("*** %-15s ***", "Customers")
	customers, err := dao.db.Query(`
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

func printCustomerProducts(dao sqlDao) {
	log.Printf("*** %-15s ***", "Customer/Products")
	customerProducts, err := dao.db.Query(`
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

func (dao sqlDao) InsertClient(name string) Client {
	log.Println("Insert client", name)
	var id int64
	err := dao.db.QueryRow(
		`INSERT INTO client (name, active)
		VALUES ($1, true)
		RETURNING id`, name).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	return NewClient(id)
}

func (dao sqlDao) InsertCustomer(code, firstName string, lastName string, email string, client Client) Customer {
	log.Println("Insert customer", firstName, lastName)
	var id int64
	err := dao.db.QueryRow(
		`INSERT INTO customer (code, first_name, last_name, email_address, client_id)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id`, code, firstName, lastName, email, client.Id).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	return NewCustomer(id)
}

func (dao sqlDao) InsertProduct(name string) Product {
	log.Println("Insert product", name)
	var id int64
	err := dao.db.QueryRow(
		`INSERT INTO product (name, active)
		VALUES ($1, true)
		RETURNING id`, name).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	return NewProduct(id)
}

func (dao sqlDao) UpdateCustomerName(customer Customer, newFullName string) {
	log.Println("Update customer", customer.Id, "name to", newFullName)
	newFirstName, newLastName, err := SplitFullName(newFullName)
	if err != nil {
		log.Fatal(err)
	}
	res, err := dao.db.Exec(
		`UPDATE customer
		SET first_name = $2
		  , last_name = $3
		WHERE id = $1`, customer.Id, newFirstName, newLastName)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Update customer name", res)
}

func (dao sqlDao) UpdateProductName(product Product, newName string) {
	log.Println("Update product", product.Id, "name to", newName)
	res, err := dao.db.Exec(
		`UPDATE product
		SET name = $2
		WHERE id = $1`, product.Id, newName)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Update product name", res)
}

func (dao sqlDao) UpdateCustomerEmailAndLinkToProduct(customer Customer, newEmail string, product Product) {
	log.Println("Update customer", customer.Id, "email address to", newEmail)
	tx, err := dao.db.Begin()
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

func (dao sqlDao) DeleteClient(client Client) {
	log.Println("Delete client", client.Id)
	_, err := dao.db.Exec(
		`DELETE FROM client
			WHERE id = $1`, client.Id)
	if err == nil {
		log.Fatal("Delete client was not blocked by DB constraints")
	}
	log.Println("Delete client was blocked by DB contraints, as expected")
}

func (dao sqlDao) UpdateClientName(client Client, newName string) {
	log.Println("Update client", client.Id, "name to", newName)
	res, err := dao.db.Exec(
		`UPDATE client
			SET name = $2
			WHERE id = $1`, client.Id, newName)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Update client name", res)
}

func (dao sqlDao) DeleteCustomer(customer Customer) {
	log.Println("Delete customer", customer.Id)
	res, err := dao.db.Exec(
		`DELETE FROM customer
			WHERE id = $1`, customer.Id)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Delete customer", res)
}

func (dao sqlDao) DeleteAllCustomers() {
	log.Println("Delete all customers")
	res, err := dao.db.Exec(`DELETE FROM customer`)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Delete all customers", res)
}

func (dao sqlDao) DeleteAllProducts() {
	log.Println("Delete all products")
	res, err := dao.db.Exec(`DELETE FROM product`)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Delete all products", res)
}

func (dao sqlDao) DeleteAllClients() {
	log.Println("Delete all clients")
	res, err := dao.db.Exec(`DELETE FROM client`)
	if err != nil {
		log.Fatal(err)
	}
	logAffectedRows("Delete all clients", res)
}

func logAffectedRows(prefix string, res sql.Result) {
	rowsAffected, _ := res.RowsAffected()
	log.Printf("%-20s: %d row(s) affected", prefix, rowsAffected)
}
