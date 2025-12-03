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
    pass


def add_user(new_user: User = None):
    """
    new_user - A User object containing a new user to be inserted into the DB in the Users table.
        new_user and its attributes will never be None.
    """
    pass


def edit_user(original_account_id: str = None, new_user: User = None):
    """
    original_account_id - A string containing the account id for the user to be edited. e.g. if the original_account_id =
        "f3aa549b042a", then all attributes that are being updated should be applied to the user in the DB with account_id ==
        "f3aa549b042a". original_account_id will never be None
    new_user - A User object containing attributes to update for a user in the database. If an attribute is None,
        then it should not be altered. e.g. if new_user.name = "John" then the user with the same account_id as
        original_account_id should have their name updated to "John". If filter_attributes.name = None, then the name should
        not be altered. new_user will never be None, but any attribute not being updated will be None.
    """
    pass


def checkout_book(isbn: str = None, account_id: str = None):
    """
    isbn - A string containing the ISBN for the book being checked out. isbn will never be None.
    account_id - A string containing the account id of the user checking out a book. account_id will never be None.
    """
    pass


def waitlist_user(isbn: str = None, account_id: str = None) -> int:
    """
    isbn - A string containing the ISBN for the book that a user desires to be waitlisted for. isbn will never be None.
    account_id - A string containing the account id for the user that wants to be waitlisted. account_id will never be None.

    returns an integer that is the user's place in line to check out the book.
    """
    return -1


def update_waitlist(isbn: str = None):
    """
    isbn - A string containing the ISBN for a book on the waitlist. isbn will never be None.
    """
    pass


def return_book(isbn: str = None, account_id: str = None):
    """
    isbn - A string containing the ISBN for the book that the user desires to return. isbn will never be None
    account_id - A string containing the account id for the user that wants to return the book. account_id will never be None
    """
    pass


def grant_extension(isbn: str = None, account_id: str = None):
    """
    isbn - A string containing the ISBN for a book. isbn will never be None.
    account_id - A string containing the account id for a user. account_id will never be None.
    """
    pass


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
    return []


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
    return []


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
    return []


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
    return []


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
    return []


def number_in_stock(isbn: str = None) -> int:
    """
    isbn - A string containing the ISBN for a book. ISBN will never be None.

    returns the quantity of books available with their ISBN equal to the isbn parameter. The quantity available should be
        calculated as how many copies the branch owns minus how many copies are checked out to users. If the library does
        not own the book, then -1 should be returned.
    """
    return 0


def place_in_line(isbn: str = None, account_id: str = None) -> int:
    """
    isbn - A string containing the ISBN for a book. ISBN will never be None.
    account_id - A string containing the account id for a user. account_id will never be None.

    returns what place in line the user with the corresponding account_id is in for the book with the corresponding ISBN. If
        the user is not on the waitlist for that book, then -1 should be returned.
    """
    return 0


def line_length(isbn: str = None) -> int:
    """
    isbn - A string containing the ISBN for a book. ISBN will never be None.

    returns how many people are on the waitlist for the book with the corresponding ISBN. e.g. if there are 5 people on the
        waitlist for a book, 5 should be returned. If there is no waitlist for the book, then 0 should be returned.
    """
    return 0


def save_changes():
    """
    Commits all changes made to the db.
    """
    pass


def close_connection():
    """
    Closes the cursor and connection.
    """
    pass
