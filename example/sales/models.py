import dorm


class Customer(dorm.Model):
    name = dorm.CharField(max_length=100)
    email = dorm.EmailField(unique=True)
    phone = dorm.CharField(max_length=20, null=True, blank=True)


class Order(dorm.Model):
    customer = dorm.ForeignKey(Customer, on_delete=dorm.CASCADE)
    order_date = dorm.DateTimeField(auto_now_add=True)
    total = dorm.DecimalField(max_digits=10, decimal_places=2)
