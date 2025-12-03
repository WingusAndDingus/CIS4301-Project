from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.LoanHistory import LoanHistory
from models.Waitlist import Waitlist
from models.Book import Book
from models.Loan import Loan
from models.User import User

UFID = "58200371"
FULLNAME = "Hernandez Martin, Fernando"

conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])#, collation='utf8mb4_unicode_ci')

cur = conn.cursor()


def add_book(new_book: Book = None):
    """
    new_book - A Book object containing a new book to be inserted into the DB in the Books table.
        new_book and its attributes will never be None.
    """
    query = """
        INSERT INTO Book (isbn, title, author, publication_year, publisher, num_owned)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    params = [
        new_book.isbn,
        new_book.title,
        new_book.author,
        new_book.publication_year,
        new_book.publisher,
        new_book.num_owned,
    ]
    cur.execute(query, params)


def add_user(new_user: User = None):
    """
    new_user - A User object containing a new user to be inserted into the DB in the Users table.
        new_user and its attributes will never be None.
    """
    query = """
        INSERT INTO User (account_id, name, address, phone_number, email)
        VALUES (?, ?, ?, ?, ?)
    """
    params = [
        new_user.account_id,
        new_user.name,
        new_user.address,
        new_user.phone_number,
        new_user.email,
    ]
    cur.execute(query, params)


def edit_user(original_account_id: str = None, new_user: User = None):
    """
    original_account_id - A string containing the account id for the user to be edited.
    new_user - A User object containing attributes to update for a user in the database.
    """
    set_clauses = []
    params = []

    if new_user.account_id is not None:
        set_clauses.append("account_id = ?")
        params.append(new_user.account_id)

    if new_user.name is not None:
        set_clauses.append("name = ?")
        params.append(new_user.name)

    if new_user.address is not None:
        set_clauses.append("address = ?")
        params.append(new_user.address)

    if new_user.phone_number is not None:
        set_clauses.append("phone_number = ?")
        params.append(new_user.phone_number)

    if new_user.email is not None:
        set_clauses.append("email = ?")
        params.append(new_user.email)

    if not set_clauses:
        return

    query = f"""
        UPDATE User
        SET {", ".join(set_clauses)}
        WHERE account_id = ?
    """
    params.append(original_account_id)

    cur.execute(query, params)


def checkout_book(isbn: str = None, account_id: str = None):
    """
    isbn - A string containing the ISBN for the book being checked out. isbn will never be None.
    account_id - A string containing the account id of the user checking out a book. account_id will never be None.
    """
    # checkout_date = current date
    # due_date = curr + 14
    query = """
        INSERT INTO Loan (isbn, account_id, checkout_date, due_date)
        VALUES (?, ?, CURRENT_DATE(), DATE_ADD(CURRENT_DATE(), INTERVAL 2 WEEK))
    """
    cur.execute(query, [isbn, account_id])


def waitlist_user(isbn: str = None, account_id: str = None) -> int:
    """
    isbn - A string containing the ISBN for the book that a user desires to be waitlisted for. isbn will never be None.
    account_id - A string containing the account id for the user that wants to be waitlisted. account_id will never be None.

    returns an integer that is the user's place in line to check out the book.
    """
    # curr max place_in_line for isbn
    cur.execute(
        "SELECT COALESCE(MAX(place_in_line), 0) FROM Waitlist WHERE isbn = ?",
        [isbn],
    )
    (current_max,) = cur.fetchone()
    new_place = current_max + 1
    cur.execute(
        """
        INSERT INTO Waitlist (isbn, account_id, place_in_line)
        VALUES (?, ?, ?)
        """,
        [isbn, account_id, new_place],
    )
    return new_place


def update_waitlist(isbn: str = None):
    """
    isbn - A string containing the ISBN for a book on the waitlist. isbn will never be None.
    """
    cur.execute(
        "DELETE FROM Waitlist WHERE isbn = ? AND place_in_line = 1",
        [isbn],
    )
    cur.execute(
        """
        UPDATE Waitlist
        SET place_in_line = place_in_line - 1
        WHERE isbn = ? AND place_in_line > 1
        """,
        [isbn],
    )


def return_book(isbn: str = None, account_id: str = None):
    """
    isbn - A string containing the ISBN for the book that the user desires to return. isbn will never be None
    account_id - A string containing the account id for the user that wants to return the book. account_id will never be None
    """
    cur.execute(
        """
        INSERT INTO LoanHistory (isbn, account_id, checkout_date, due_date, return_date)
        SELECT isbn, account_id, checkout_date, due_date, CURRENT_DATE()
        FROM Loan
        WHERE isbn = ? AND account_id = ?
        """,
        [isbn, account_id],
    )
    # Delete loan entry bc the book is returned
    cur.execute(
        "DELETE FROM Loan WHERE isbn = ? AND account_id = ?",
        [isbn, account_id],
    )



def grant_extension(isbn: str = None, account_id: str = None):
    """
    isbn - A string containing the ISBN for a book. isbn will never be None.
    account_id - A string containing the account id for a user. account_id will never be None.
    """
    cur.execute(
        """
        UPDATE Loan
        SET due_date = DATE_ADD(due_date, INTERVAL 2 WEEK)
        WHERE isbn = ? AND account_id = ?
        """,
        [isbn, account_id],
    )


def get_filtered_books(filter_attributes: Book = None,
                       use_patterns: bool = False,
                       min_publication_year: int = -1,
                       max_publication_year: int = -1) -> list[Book]:
    """
    filter_attributes - A Book object containing attributes to filter books in the database. If an attribute is None,
        then it should not be considered for the search. e.g. if filter_attributes.title = "1984" then all books returned
        should have their title == "1984". If filter_attributes.author = None, then we do not care what the author is when
        filtering. It is important to note that filter_attributes.publication_year will always be -1 since we have
        separate parameters to handle publication_year. It is also worth noting that since num_owned is an integer, it
        can't be None, so it will default to -1 instead when not used. Additionally, many attributes may be used as a
        filter simultaneously. filter_attributes will never be None, but any attribute not being used as a filter will be None.
        It is also possible all the attributes in filter_attributes to be None, if that is the case then all rows should be returned.
    use_patterns - If True, then the string attributes in filter_attributes may contain string patterns rather than typical
        string literals, so the filtering should handle this accordingly. e.g. if filter_attributes.title = "The Great%" and
        use_patterns = True, then all Books returned should have their title start with "The Great%". If use_patterns = False,
        then all books returned should have their title == "The Great%".
    min_publication_year - The minimum publication year to filter books by, inclusively. e.g. if min_publication_year = 2000,
        then all books should be published between 2000 and the current year, including 2000 and the current year. If
        min_publication_year is not used, it will be -1.
    max_publication_year - The maximum publication year to filter books by, inclusively. e.g. if max_publication_year = 1999,
        then all books should be published before the year 2000, not including 2000. If max_publication_year is not used,
        it will be -1.

    returns a list of Book objects with books that meet the qualifications of the filtered attributes. If no books meet the
        requirements, then an empty list is returned.
    """
    #qury definition
    query = """
        SELECT isbn, title, author, publication_year, publisher, num_owned
        FROM Book
    """
    conditions = []
    params = []

    # String attributes
    if filter_attributes.isbn is not None:
        if use_patterns:
            conditions.append("isbn LIKE ?")
        else:
            conditions.append("isbn = ?")
        params.append(filter_attributes.isbn)

    if filter_attributes.title is not None:
        if use_patterns:
            conditions.append("title LIKE ?")
        else:
            conditions.append("title = ?")
        params.append(filter_attributes.title)

    if filter_attributes.author is not None:
        if use_patterns:
            conditions.append("author LIKE ?")
        else:
            conditions.append("author = ?")
        params.append(filter_attributes.author)

    if filter_attributes.publisher is not None:
        if use_patterns:
            conditions.append("publisher LIKE ?")
        else:
            conditions.append("publisher = ?")
        params.append(filter_attributes.publisher)

    # num_owned (int; -1 means "ignore")
    if getattr(filter_attributes, "num_owned", -1) != -1:
        conditions.append("num_owned = ?")
        params.append(filter_attributes.num_owned)

    # Publication year range
    if min_publication_year != -1:
        conditions.append("publication_year >= ?")
        params.append(min_publication_year)

    if max_publication_year != -1:
        conditions.append("publication_year <= ?")
        params.append(max_publication_year)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    cur.execute(query, params)
    rows = cur.fetchall()

    books: list[Book] = []
    for isbn, title, author, pub_year, publisher, num_owned in rows:
        books.append(
            Book(
                isbn=isbn,
                title=title,
                author=author,
                publication_year=pub_year,
                publisher=publisher,
                num_owned=num_owned,
            )
        )

    return books


def get_filtered_users(filter_attributes: User = None, use_patterns: bool = False) -> list[User]:
    """
    filter_attributes - A User object containing attributes to filter users in the database. If an attribute is None,
        then it should not be considered for the search. e.g. if filter_attributes.name = "John" then all users returned
        should have their name == "John". If filter_attributes.address = None, then we do not care what the address is when
        filtering. Additionally, many attributes may be used as a filter simultaneously. filter_attributes will never be
        None, but any attribute not being used as a filter will be None. It is also possible all the attributes in
        filter_attributes to be None, if that is the case then all rows should be returned.
    use_patterns - If True, then the string attributes in filter_attributes may contain string patterns rather than typical
        string literals, so the search should handle this accordingly. e.g. if filter_attributes.name = "John%" and
        use_patterns = True, then all Users returned should have their name start with "John". If use_patterns = False, then
        all users returned should have their name == "John%".

    returns a list of User objects with users who meet the qualifications of the filters. If no users meet the requirements,
     then an empty list is returned.
    """
    # query definition
    query = """
        SELECT account_id, name, address, phone_number, email
        FROM User
    """
    conditions = []
    params = []

    if filter_attributes.account_id is not None:
        if use_patterns:
            conditions.append("account_id LIKE ?")
        else:
            conditions.append("account_id = ?")
        params.append(filter_attributes.account_id)

    if filter_attributes.name is not None:
        if use_patterns:
            conditions.append("name LIKE ?")
        else:
            conditions.append("name = ?")
        params.append(filter_attributes.name)

    if filter_attributes.address is not None:
        if use_patterns:
            conditions.append("address LIKE ?")
        else:
            conditions.append("address = ?")
        params.append(filter_attributes.address)

    if filter_attributes.phone_number is not None:
        if use_patterns:
            conditions.append("phone_number LIKE ?")
        else:
            conditions.append("phone_number = ?")
        params.append(filter_attributes.phone_number)

    if filter_attributes.email is not None:
        if use_patterns:
            conditions.append("email LIKE ?")
        else:
            conditions.append("email = ?")
        params.append(filter_attributes.email)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    cur.execute(query, params)
    rows = cur.fetchall()

    users: list[User] = []
    for account_id, name, address, phone_number, email in rows:
        users.append(
            User(
                account_id=account_id,
                name=name,
                address=address,
                phone_number=phone_number,
                email=email,
            )
        )

    return users

def get_filtered_loans(filter_attributes: Loan = None,
                       min_checkout_date: str = None,
                       max_checkout_date: str = None,
                       min_due_date: str = None,
                       max_due_date: str = None, ) -> list[Loan]:
    """
    filter_attributes - A Loan object containing attributes to filter loan in the database. If an attribute is None,
        then it should not be considered for the search. e.g. if filter_attributes.isbn = "123456789" then all loans returned
        should have their isbn == "123456789". If filter_attributes.isbn = None, then we do not care what the isbn is, when
        filtering. Additionally, many attributes may be used as a filter simultaneously. filter_attributes will never be
        None, but any attribute not being used as a filter will be None. It is also possible all the attributes in
        filter_attributes to be None, if that is the case then all rows should be returned.
    min_checkout_date - The minimum checkout date (formatted in YYYY-mm-dd) to filter loans by, inclusively. e.g. if
        min_checkout_date = "2025-01-02", then all loans should be checked out after "2025-01-01", not including
        "2025-01-01". If min_checkout_date is not used, it will be None
    max_checkout_date - The maximum checkout date (formatted in YYYY-mm-dd) to filter loans by, inclusively. e.g. if
        max_checkout_date = "2025-01-02", then all loans should be checked out before "2025-01-03", not including
        "2025-01-03". If max_checkout_date is not used, it will be None
    min_due_date - like min_checkout_date but with the due date instead. If min_due_date is not used, it will be None.
    max_due_date - like max_checkout_date but with the due date instead. If max_due_date is not used, it will be None.

    returns a list of Loan objects with loans that meet the qualifications of the filters. If no loans meet the
    requirements, then an empty list is returned.
    """
    #define query
    query = """
        SELECT isbn, account_id, checkout_date, due_date
        FROM Loan
    """
    conditions = []
    params = []

    if filter_attributes.isbn is not None:
        conditions.append("isbn = ?")
        params.append(filter_attributes.isbn)

    if filter_attributes.account_id is not None:
        conditions.append("account_id = ?")
        params.append(filter_attributes.account_id)

    if getattr(filter_attributes, "checkout_date", None) is not None:
        conditions.append("checkout_date = ?")
        params.append(filter_attributes.checkout_date)

    if getattr(filter_attributes, "due_date", None) is not None:
        conditions.append("due_date = ?")
        params.append(filter_attributes.due_date)

    # Date ranges
    if min_checkout_date is not None:
        conditions.append("checkout_date >= ?")
        params.append(min_checkout_date)

    if max_checkout_date is not None:
        conditions.append("checkout_date <= ?")
        params.append(max_checkout_date)

    if min_due_date is not None:
        conditions.append("due_date >= ?")
        params.append(min_due_date)

    if max_due_date is not None:
        conditions.append("due_date <= ?")
        params.append(max_due_date)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    cur.execute(query, params)
    rows = cur.fetchall()

    loans: list[Loan] = []
    for isbn, account_id, checkout_date, due_date in rows:
        loans.append(
            Loan(
                isbn=isbn,
                account_id=account_id,
                checkout_date=checkout_date.isoformat() if checkout_date else None,
                due_date=due_date.isoformat() if due_date else None,
            )
        )

    return loans


def get_filtered_loan_histories(filter_attributes: LoanHistory = None,
                                min_checkout_date: str = None,
                                max_checkout_date: str = None,
                                min_due_date: str = None,
                                max_due_date: str = None,
                                min_return_date: str = None,
                                max_return_date: str = None) -> list[LoanHistory]:
    """
    filter_attributes - A LoanHistory object containing attributes to filter loan histories in the database. If an attribute is None,
        then it should not be considered for the search. e.g. if filter_attributes.isbn = "123456789" then all rows returned
        should have their isbn == "123456789". If filter_attributes.isbn = None, then we do not care what the isbn is when
        filtering. Additionally, many attributes may be used as a filter simultaneously. filter_attributes will never be
        None, but any attribute not being used as a filter will be None. It is also possible all the attributes in
        filter_attributes to be None, if that is the case then all rows should be returned.
    min_checkout_date - The minimum checkout date (formatted in YYYY-mm-dd) to filter loans by, inclusively. e.g. if
        min_checkout_date = "2025-01-02", then all loans should be checked out after "2025-01-01", not including
        "2025-01-01". If min_checkout_date is not used, it will be None
    max_checkout_date - The maximum checkout date (formatted in YYYY-mm-dd) to filter loans by, inclusively. e.g. if
        max_checkout_date = "2025-01-02", then all loans should be checked out before "2025-01-03", not including
        "2025-01-03". If max_checkout_date is not used, it will be None
    min_due_date - like min_checkout_date but with the due date instead. If min_due_date is not used, it will be None.
    max_due_date - like max_checkout_date but with the due date instead. If max_due_date is not used, it will be None.
    min_return_date - like min_checkout_date but with the return date instead. If min_return_date is not used, it will be
        None.
    max_return_date - like max_checkout_date but with the return date instead. If max_return_date is not used, it will be
        None.

    returns a list of LoanHistory objects with return entries that meet the qualifications of the filters. If no entries
    meet the requirements, then an empty list is returned
    """
    # query def
    query = """
        SELECT isbn, account_id, checkout_date, due_date, return_date
        FROM LoanHistory
    """
    conditions = []
    params = []

    if filter_attributes.isbn is not None:
        conditions.append("isbn = ?")
        params.append(filter_attributes.isbn)

    if filter_attributes.account_id is not None:
        conditions.append("account_id = ?")
        params.append(filter_attributes.account_id)

    if getattr(filter_attributes, "checkout_date", None) is not None:
        conditions.append("checkout_date = ?")
        params.append(filter_attributes.checkout_date)

    if getattr(filter_attributes, "due_date", None) is not None:
        conditions.append("due_date = ?")
        params.append(filter_attributes.due_date)

    if getattr(filter_attributes, "return_date", None) is not None:
        conditions.append("return_date = ?")
        params.append(filter_attributes.return_date)

    # Date ranges
    if min_checkout_date is not None:
        conditions.append("checkout_date >= ?")
        params.append(min_checkout_date)

    if max_checkout_date is not None:
        conditions.append("checkout_date <= ?")
        params.append(max_checkout_date)

    if min_due_date is not None:
        conditions.append("due_date >= ?")
        params.append(min_due_date)

    if max_due_date is not None:
        conditions.append("due_date <= ?")
        params.append(max_due_date)

    if min_return_date is not None:
        conditions.append("return_date >= ?")
        params.append(min_return_date)

    if max_return_date is not None:
        conditions.append("return_date <= ?")
        params.append(max_return_date)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    cur.execute(query, params)
    rows = cur.fetchall()

    histories: list[LoanHistory] = []
    for isbn, account_id, checkout_date, due_date, return_date in rows:
        histories.append(
            LoanHistory(
                isbn=isbn,
                account_id=account_id,
                checkout_date=checkout_date.isoformat() if checkout_date else None,
                due_date=due_date.isoformat() if due_date else None,
                return_date=return_date.isoformat() if return_date else None,
            )
        )

    return histories



def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    filter_attributes - A Waitlist object containing attributes to filter waitlists in the database. If an attribute is None,
        then it should not be considered for the search. e.g. if filter_attributes.isbn = "123456789" then all rows returned
        should have their isbn == "123456789". If filter_attributes.isbn = None, then we do not care what the isbn is when
        filtering. Additionally, many attributes may be used as a filter simultaneously. filter_attributes will never be
        None, but any attribute not being used as a filter will be None. It is also possible all the attributes in
        filter_attributes to be None, if that is the case then all rows should be returned.
    min_place_in_line - The minimum place in line for a waitlist to be. e.g. if min_place_in_line = 3 then only entries
        where the place_in_line is greater than or equal to 3 should be included. If min_place_in_line is not used, it will
        be -1.
    max_place_in_line - The minimum place in line for a waitlist to be. e.g. if max_place_in_line = 3 then only entries
        where the place_in_line is less than or equal to 3 should be included. If max_place_in_line is not used, it will be
         -1.

    returns a list of Waitlist objects with waitlist entries that meet the qualifications of the filters. If no entries meet
     the requirements, then an empty list is returned.
    """
    # define query
    query = """
        SELECT isbn, account_id, place_in_line
        FROM Waitlist
    """
    conditions = []
    params = []

    if filter_attributes.isbn is not None:
        conditions.append("isbn = ?")
        params.append(filter_attributes.isbn)

    if filter_attributes.account_id is not None:
        conditions.append("account_id = ?")
        params.append(filter_attributes.account_id)

    if getattr(filter_attributes, "place_in_line", -1) != -1:
        conditions.append("place_in_line = ?")
        params.append(filter_attributes.place_in_line)

    if min_place_in_line != -1:
        conditions.append("place_in_line >= ?")
        params.append(min_place_in_line)

    if max_place_in_line != -1:
        conditions.append("place_in_line <= ?")
        params.append(max_place_in_line)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    cur.execute(query, params)
    rows = cur.fetchall()

    entries: list[Waitlist] = []
    for isbn, account_id, place_in_line in rows:
        entries.append(
            Waitlist(
                isbn=isbn,
                account_id=account_id,
                place_in_line=place_in_line,
            )
        )

    return entries


def number_in_stock(isbn: str = None) -> int:
    """
    isbn - A string containing the ISBN for a book. ISBN will never be None.

    returns the quantity of books available with their ISBN equal to the isbn parameter. The quantity available should be
        calculated as how many copies the branch owns minus how many copies are checked out to users. If the library does
        not own the book, then -1 should be returned.
    """
    # how many library owns
    cur.execute(
        "SELECT num_owned FROM Book WHERE isbn = ?",
        [isbn],
    )
    row = cur.fetchone()
    if row is None:
        return -1  # doesn't own the book

    (num_owned,) = row

    # count how many currently checked out
    cur.execute(
        "SELECT COUNT(*) FROM Loan WHERE isbn = ?",
        [isbn],
    )
    (num_checked_out,) = cur.fetchone()

    return num_owned - num_checked_out


def place_in_line(isbn: str = None, account_id: str = None) -> int:
    """
    isbn - A string containing the ISBN for a book. ISBN will never be None.
    account_id - A string containing the account id for a user. account_id will never be None.

    returns what place in line the user with the corresponding account_id is in for the book with the corresponding ISBN. If
        the user is not on the waitlist for that book, then -1 should be returned.
    """
    cur.execute(
        """
        SELECT place_in_line
        FROM Waitlist
        WHERE isbn = ? AND account_id = ?
        """,
        [isbn, account_id],
    )
    row = cur.fetchone()
    if row is None:
        return -1

    (place,) = row
    return place


def line_length(isbn: str = None) -> int:
    """
    isbn - A string containing the ISBN for a book. ISBN will never be None.

    returns how many people are on the waitlist for the book with the corresponding ISBN. e.g. if there are 5 people on the
        waitlist for a book, 5 should be returned. If there is no waitlist for the book, then 0 should be returned.
    """
    cur.execute(
        "SELECT COUNT(*) FROM Waitlist WHERE isbn = ?",
        [isbn],
    )
    (count,) = cur.fetchone()
    return count


def save_changes():
    """
    Commits all changes made to the db.
    """
    conn.commit()


def close_connection():
    """
    Closes the cursor and connection.
    """
    try:
        cur.close()
    finally:
        conn.close()
