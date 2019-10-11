CREATE TABLE viertel(
        plz INTEGER PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        insert_date DATE NOT NULL DEFAULT CURRENT_DATE 
);
