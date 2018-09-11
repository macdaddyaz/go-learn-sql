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
	. "go-learn-sql/common"
	. "go-learn-sql/sql"
	"log"
)

func main() {
	Data = OpenDb()
	defer Data.Close()
	log.Print(Data)

	clients := [2]Client{
		InsertClient("Los Angeles Lakers"),
		InsertClient("Boston Celtics"),
	}
	customers := [8]Customer{
		InsertCustomer("123", "Kobe", "Bryant", "kbryant8@lakers.com", clients[0]),
		InsertCustomer("234", "Shaquille", "O'Neal", "soneal@lakers.com", clients[0]),
		InsertCustomer("345", "Magic", "Johnson", "mjohnson@lakers.com", clients[0]),
		InsertCustomer("456", "Kareem", "Abdul-Jabbar", "kabduljabbar@lakers.com", clients[0]),
		InsertCustomer("567", "Jerry", "West", "jwest@lakers.com", clients[0]),
		InsertCustomer("678", "Bill", "Russell", "brussel@celtics.com", clients[1]),
		InsertCustomer("789", "Larry", "Bird", "lbird@celtics.com", clients[1]),
		InsertCustomer("890", "Paul", "Pierce", "ppierce@celtics.com", clients[1]),
	}
	products := [3]Product{
		InsertProduct("Super Personal Resolution Service"),
		InsertProduct("Fantastic Identity Monitoring"),
		InsertProduct("Watching Some Other Stuff"),
	}
	PrintDatabaseState()

	UpdateCustomerName(customers[3], "Lew Alcindor")
	UpdateProductName(products[2], "Stupendous Cyber Monitoring")
	UpdateCustomerEmailAndLinkToProduct(customers[4], "jwest@clippers.com", products[1])
	DeleteClient(clients[1])
	UpdateClientName(clients[1], "Evil Empire")
	DeleteCustomer(customers[7])
	PrintDatabaseState()

	DeleteAllCustomers()
	DeleteAllProducts()
	DeleteAllClients()
	PrintDatabaseState()
}
