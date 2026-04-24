import dorm


class Publisher(dorm.Model):
    name = dorm.CharField(max_length=100)

    class Meta:
        db_table = "publishers"


class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()
    email = dorm.EmailField(null=True, blank=True)
    is_active = dorm.BooleanField(default=True)
    publisher = dorm.ForeignKey(
        Publisher, on_delete=dorm.SET_NULL, null=True, blank=True
    )

    class Meta:
        db_table = "authors"


class Book(dorm.Model):
    title = dorm.CharField(max_length=200)
    author = dorm.ForeignKey(Author, on_delete=dorm.CASCADE)
    author_id: int | None
    pages = dorm.IntegerField(default=0)
    published = dorm.BooleanField(default=False)

    class Meta:
        db_table = "books"


class Tag(dorm.Model):
    name = dorm.CharField(max_length=50, unique=True)

    class Meta:
        db_table = "tags"


class Article(dorm.Model):
    title = dorm.CharField(max_length=200)
    tags = dorm.ManyToManyField(Tag, related_name="articles")

    class Meta:
        db_table = "articles"
