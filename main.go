package main

// Seed dal
//   1. Insert 2 clients
//   2. Insert 8 customers
//   3. Insert 3 products
//   4. Link all customers to 1 or 2 random products
// Find and update some dal
//   1. Find customer and update name
//   2. Find product and update name
//   3. Find customer, update email, and create link to new product (transaction)
//   4. Find client and delete it (should produce error)
//   5. Find client and update name
//   6. Find customer and delete it
// Cleanup dal
//   1. Delete customers (should delete customer/product links too)
//   2. Delete products
//   3. Delete clients

import (
	. "go-learn-sql/common"
	/// Experiment with database access using only Go's database/dal package
	/// Using documentation from http://go-database-sql.org/
	// dal "go-learn-sql/sql"
	/// Experiment with database access using GORM (http://gorm.io/)
	dal "go-learn-sql/gorm"
	/// Experiment with database access using the Upper DB v3 library (https://upper.io/db.v3)
	// dal "go-learn-sql/upper"
	/// Experiment with database access using SQLX (http://jmoiron.github.io/sqlx/)
	// dal "go-learn-sql/sqlx"
	/// Experiment with database access using GoCraft DBR (https://github.com/gocraft/dbr)
	// dal "go-learn-sql/dbr"
	/// Experiment with database access using Data Access Kit (https://github.com/mgutz/dat)
	// dal "go-learn-sql/dat"
	/// Experiment with database access using PostgreSQL ORM (https://github.com/go-pg/pg)
	// dal "go-learn-sql/gopg"
)

func main() {
	dao := dal.Init()
	defer dao.Shutdown()

	clients := [2]Client{
		dao.InsertClient("Los Angeles Lakers"),
		dao.InsertClient("Boston Celtics"),
	}
	customers := [8]Customer{
		dao.InsertCustomer("123", "Kobe", "Bryant", "kbryant8@lakers.com", clients[0]),
		dao.InsertCustomer("234", "Shaquille", "O'Neal", "soneal@lakers.com", clients[0]),
		dao.InsertCustomer("345", "Magic", "Johnson", "mjohnson@lakers.com", clients[0]),
		dao.InsertCustomer("456", "Kareem", "Abdul-Jabbar", "kabduljabbar@lakers.com", clients[0]),
		dao.InsertCustomer("567", "Jerry", "West", "jwest@lakers.com", clients[0]),
		dao.InsertCustomer("678", "Bill", "Russell", "brussel@celtics.com", clients[1]),
		dao.InsertCustomer("789", "Larry", "Bird", "lbird@celtics.com", clients[1]),
		dao.InsertCustomer("890", "Paul", "Pierce", "ppierce@celtics.com", clients[1]),
	}
	products := [3]Product{
		dao.InsertProduct("Super Personal Resolution Service"),
		dao.InsertProduct("Fantastic Identity Monitoring"),
		dao.InsertProduct("Watching Some Other Stuff"),
	}
	dao.PrintDatabaseState()

	dao.UpdateCustomerName(customers[3], "Lew Alcindor")
	dao.UpdateProductName(products[2], "Stupendous Cyber Monitoring")
	dao.UpdateCustomerEmailAndLinkToProduct(customers[4], "jwest@clippers.com", products[1])
	dao.DeleteClient(clients[1])
	dao.UpdateClientName(clients[1], "Evil Empire")
	dao.DeleteCustomer(customers[7])
	dao.PrintDatabaseState()

	dao.DeleteAllCustomers()
	dao.DeleteAllProducts()
	dao.DeleteAllClients()
	dao.PrintDatabaseState()
}
