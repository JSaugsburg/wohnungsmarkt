CREATE TABLE cities(
        name VARCHAR(255) PRIMARY KEY,
        insert_date DATE NOT NULL DEFAULT CURRENT_DATE,
        entries_count integer
);
