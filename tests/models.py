import dorm


class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()
    email = dorm.EmailField(null=True, blank=True)
    is_active = dorm.BooleanField(default=True)

    class Meta:
        db_table = "authors"


class Book(dorm.Model):
    title = dorm.CharField(max_length=200)
    author = dorm.ForeignKey(Author, on_delete=dorm.CASCADE)
    pages = dorm.IntegerField(default=0)
    published = dorm.BooleanField(default=False)

    class Meta:
        db_table = "books"
