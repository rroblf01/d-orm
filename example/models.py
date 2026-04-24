import dorm


class Genre(dorm.Model):
    name        = dorm.CharField(max_length=50, unique=True)
    description = dorm.TextField(null=True, blank=True)

    class Meta:
        db_table = "genres"


class Author(dorm.Model):
    name        = dorm.CharField(max_length=100)
    email       = dorm.EmailField(unique=True)
    birth_year  = dorm.IntegerField(null=True)
    nationality = dorm.CharField(max_length=60, null=True, blank=True)
    active      = dorm.BooleanField(default=True)
    bio         = dorm.TextField(null=True, blank=True)

    class Meta:
        db_table = "authors"


class Book(dorm.Model):
    title          = dorm.CharField(max_length=200)
    author         = dorm.ForeignKey(Author, on_delete=dorm.CASCADE)
    genre          = dorm.ForeignKey(Genre,  on_delete=dorm.SET_NULL, null=True)
    isbn           = dorm.CharField(max_length=13, unique=True)
    pages          = dorm.IntegerField()
    price          = dorm.DecimalField(max_digits=8, decimal_places=2)
    published_year = dorm.IntegerField()
    stock          = dorm.IntegerField(default=0)
    available      = dorm.BooleanField(default=True)

    class Meta:
        db_table = "books"


class Review(dorm.Model):
    book       = dorm.ForeignKey(Book, on_delete=dorm.CASCADE)
    reviewer   = dorm.CharField(max_length=100)
    rating     = dorm.IntegerField()   # 1 – 5
    comment    = dorm.TextField(null=True, blank=True)
    created_at = dorm.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "reviews"
