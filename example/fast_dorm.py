"""FastAPI example using dorm + DormSchema.

DormSchema (Django-REST-style):
  - Inner ``class Meta`` chooses the dorm Model and the field set.
  - Anything you declare in the class body — extra fields, type
    overrides, ``@field_validator`` decorators — wins over the
    Meta-derived defaults.
  - ``from_attributes=True`` is on by default, so FastAPI can serialize
    a dorm instance directly via ``response_model=YourSchema``.
"""

from fastapi import FastAPI

from dorm.contrib.pydantic import DormSchema

from .sales.models import Customer

app = FastAPI()


# ── Schemas ───────────────────────────────────────────────────────────────────


class CustomerOut(DormSchema):
    """Response schema — every Customer column."""

    class Meta:
        model = Customer
        fields = "__all__"


class CustomerCreate(DormSchema):
    class Meta:
        model = Customer
        exclude = ("id",)


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/customer/", response_model=CustomerOut)
def create_customer(payload: CustomerCreate):
    customer = Customer(**payload.model_dump())
    customer.save()
    return customer


@app.get("/customer/{customer_id}", response_model=CustomerOut)
def get_customer(customer_id: int):
    return Customer.objects.get(id=customer_id)


@app.get("/customers/", response_model=list[CustomerOut])
def list_customers():
    return Customer.objects.all()
