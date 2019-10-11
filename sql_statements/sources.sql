CREATE TABLE sources(
        name VARCHAR(255) PRIMARY KEY,
        url VARCHAR(255) NOT NULL,
        insert_date DATE NOT NULL DEFAULT CURRENT_DATE 
);
