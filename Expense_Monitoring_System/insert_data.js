// Switch to database (or create if it doesn't exist)
db = db.getSiblingDB("personal_expenses");

// Insert multiple documents (expenses)
db.receipts.insertMany([
    {
        user_id: 1,
        date: new Date("2025-01-05"),
        category: "Food",
        amount: 250.50,
        notes: "Groceries at Walmart",
        scanned_receipt: {
            store: "Walmart",
            items: [
                { name: "Milk", price: 50.00 },
                { name: "Bread", price: 30.00 }
            ]
        }
    },
    {
        user_id: 2,
        date: new Date("2025-02-15"),
        category: "Entertainment",
        amount: 200.00,
        notes: "Movie tickets",
        scanned_receipt: {
            theater: "PVR Cinemas",
            seats: 2
        }
    }
]);

// Create indexes for fast searching
db.receipts.createIndex({ user_id: 1 });
db.receipts.createIndex({ date: 1 });
